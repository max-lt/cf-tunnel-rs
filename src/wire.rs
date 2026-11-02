//! The part of the Cap'n Proto wire format this crate needs.
//!
//! The reader accepts any valid message: several segments, far pointers,
//! structs larger or smaller than expected. The writer emits one segment.
//! Capabilities and non-composite lists of structs are not supported.

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::Error;

const WORD: usize = 8;
const MAX_SEGMENTS: usize = 64;
/// A message may hold at most 8 MiB. Edge messages are far smaller.
const MAX_WORDS: usize = 1 << 20;

fn bad(what: &str) -> Error {
    Error::Protocol(format!("malformed message: {what}"))
}

/// Pointer kinds, in the low two bits of a pointer word.
const STRUCT: u64 = 0;
const LIST: u64 = 1;
const FAR: u64 = 2;

/// List element size codes.
const BYTES: u64 = 2;
const COMPOSITE: u64 = 7;

/// A message that was read from a stream.
pub(crate) struct Message {
    segments: Vec<Vec<u8>>,
}

/// An object found by following a pointer.
struct Raw {
    seg: usize,
    /// Byte offset of the object.
    start: usize,
    /// The pointer word that describes the object. Its offset field is unused.
    tag: u64,
}

/// Reads one framed message: a segment table, then the segments.
pub(crate) async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Message, Error> {
    let mut first = [0u8; 4];
    reader.read_exact(&mut first).await?;

    let count = u32::from_le_bytes(first) as usize + 1;
    if count > MAX_SEGMENTS {
        return Err(bad("too many segments"));
    }

    // The table holds `count` sizes and is padded to a whole word.
    let mut table = vec![0u8; count * 4 + if count % 2 == 0 { 4 } else { 0 }];
    reader.read_exact(&mut table).await?;

    let mut sizes = Vec::with_capacity(count);
    let mut total = 0usize;
    for chunk in table.chunks_exact(4).take(count) {
        let words = u32::from_le_bytes(chunk.try_into().expect("4 bytes")) as usize;
        total = total
            .checked_add(words)
            .ok_or_else(|| bad("size overflow"))?;
        sizes.push(words);
    }

    if total > MAX_WORDS {
        return Err(bad("message too large"));
    }

    let mut segments = Vec::with_capacity(count);
    for words in sizes {
        let mut segment = vec![0u8; words * WORD];
        reader.read_exact(&mut segment).await?;
        segments.push(segment);
    }

    Ok(Message { segments })
}

impl Message {
    /// The root struct of the message.
    pub(crate) fn root(&self) -> Result<Struct<'_>, Error> {
        self.struct_at(0, 0)?.ok_or_else(|| bad("empty root"))
    }

    fn word(&self, seg: usize, at: usize) -> Result<u64, Error> {
        let bytes = self
            .segments
            .get(seg)
            .and_then(|s| s.get(at..at.checked_add(WORD)?))
            .ok_or_else(|| bad("pointer out of bounds"))?;

        Ok(u64::from_le_bytes(bytes.try_into().expect("8 bytes")))
    }

    /// The byte offset a struct or list pointer at `at` points to.
    fn near_target(at: usize, word: u64) -> Result<usize, Error> {
        // The offset is a signed 30-bit count of words after the pointer.
        let offset = i64::from((word as u32 as i32) >> 2);
        let start = at as i64 + WORD as i64 + offset * WORD as i64;

        usize::try_from(start).map_err(|_| bad("negative pointer offset"))
    }

    /// Follows the pointer at byte `at` of segment `seg`, through far pointers.
    fn follow(&self, seg: usize, at: usize) -> Result<Option<Raw>, Error> {
        let word = self.word(seg, at)?;

        if word == 0 {
            return Ok(None);
        }

        match word & 3 {
            STRUCT | LIST => Ok(Some(Raw {
                seg,
                start: Self::near_target(at, word)?,
                tag: word,
            })),

            FAR => {
                let double = (word >> 2) & 1 == 1;
                let pad = ((word >> 3) & 0x1fff_ffff) as usize * WORD;
                let pad_seg = (word >> 32) as usize;
                let first = self.word(pad_seg, pad)?;

                // A single far pointer lands on an ordinary pointer.
                if !double {
                    if first & 3 > LIST {
                        return Err(bad("far pointer lands on a non-object"));
                    }

                    return Ok(Some(Raw {
                        seg: pad_seg,
                        start: Self::near_target(pad, first)?,
                        tag: first,
                    }));
                }

                // A double far pointer lands on a far pointer to the content,
                // followed by a tag word that describes it.
                let tag = self.word(pad_seg, pad + WORD)?;
                if first & 3 != FAR || (first >> 2) & 1 == 1 || tag & 3 > LIST {
                    return Err(bad("invalid double far pointer"));
                }

                Ok(Some(Raw {
                    seg: (first >> 32) as usize,
                    start: ((first >> 3) & 0x1fff_ffff) as usize * WORD,
                    tag,
                }))
            }

            _ => Err(bad("capability pointer")),
        }
    }

    fn check(&self, seg: usize, start: usize, len: usize) -> Result<(), Error> {
        let end = start.checked_add(len).ok_or_else(|| bad("size overflow"))?;
        let segment = self.segments.get(seg).ok_or_else(|| bad("bad segment"))?;

        if end > segment.len() {
            return Err(bad("object out of bounds"));
        }

        Ok(())
    }

    fn struct_at(&self, seg: usize, at: usize) -> Result<Option<Struct<'_>>, Error> {
        let Some(raw) = self.follow(seg, at)? else {
            return Ok(None);
        };

        if raw.tag & 3 != STRUCT {
            return Err(bad("expected a struct"));
        }

        let data_len = ((raw.tag >> 32) & 0xffff) as usize * WORD;
        let pointers = (raw.tag >> 48) as usize;
        self.check(raw.seg, raw.start, data_len + pointers * WORD)?;

        Ok(Some(Struct {
            msg: self,
            seg: raw.seg,
            start: raw.start,
            data_len,
            pointers,
        }))
    }

    /// The bytes of a byte list: `Data`, or `Text` including its terminator.
    fn bytes_at(&self, seg: usize, at: usize) -> Result<&[u8], Error> {
        let Some(raw) = self.follow(seg, at)? else {
            return Ok(&[]);
        };

        if raw.tag & 3 != LIST || (raw.tag >> 32) & 7 != BYTES {
            return Err(bad("expected a byte list"));
        }

        let len = (raw.tag >> 35) as usize;
        self.check(raw.seg, raw.start, len)?;

        Ok(&self.segments[raw.seg][raw.start..raw.start + len])
    }

    fn text_at(&self, seg: usize, at: usize) -> Result<&str, Error> {
        let bytes = self.bytes_at(seg, at)?;

        // A null pointer is the empty string. Otherwise a NUL ends the text.
        let Some((&0, text)) = bytes.split_last() else {
            return if bytes.is_empty() {
                Ok("")
            } else {
                Err(bad("text is not NUL-terminated"))
            };
        };

        std::str::from_utf8(text).map_err(|_| bad("text is not UTF-8"))
    }
}

/// A struct inside a [`Message`]. A field beyond the stored size reads as zero.
#[derive(Clone, Copy)]
pub(crate) struct Struct<'a> {
    msg: &'a Message,
    seg: usize,
    start: usize,
    data_len: usize,
    pointers: usize,
}

impl<'a> Struct<'a> {
    fn data<const N: usize>(&self, offset: usize) -> [u8; N] {
        let mut out = [0u8; N];

        if offset + N <= self.data_len {
            let from = self.start + offset;
            out.copy_from_slice(&self.msg.segments[self.seg][from..from + N]);
        }

        out
    }

    pub(crate) fn u8(&self, offset: usize) -> u8 {
        self.data::<1>(offset)[0]
    }

    pub(crate) fn u16(&self, offset: usize) -> u16 {
        u16::from_le_bytes(self.data(offset))
    }

    pub(crate) fn u32(&self, offset: usize) -> u32 {
        u32::from_le_bytes(self.data(offset))
    }

    pub(crate) fn i64(&self, offset: usize) -> i64 {
        i64::from_le_bytes(self.data(offset))
    }

    pub(crate) fn bool(&self, bit: usize) -> bool {
        self.u8(bit / 8) >> (bit % 8) & 1 == 1
    }

    /// Byte offset of pointer `index`, or `None` beyond the stored pointers.
    fn slot(&self, index: usize) -> Option<usize> {
        (index < self.pointers).then(|| self.start + self.data_len + index * WORD)
    }

    pub(crate) fn text(&self, index: usize) -> Result<&'a str, Error> {
        match self.slot(index) {
            Some(at) => self.msg.text_at(self.seg, at),
            None => Ok(""),
        }
    }

    pub(crate) fn data_bytes(&self, index: usize) -> Result<&'a [u8], Error> {
        match self.slot(index) {
            Some(at) => self.msg.bytes_at(self.seg, at),
            None => Ok(&[]),
        }
    }

    /// The struct behind pointer `index`, or `None` when it is null.
    pub(crate) fn structure(&self, index: usize) -> Result<Option<Struct<'a>>, Error> {
        match self.slot(index) {
            Some(at) => self.msg.struct_at(self.seg, at),
            None => Ok(None),
        }
    }

    /// A list of structs. A null pointer is an empty list.
    pub(crate) fn struct_list(&self, index: usize) -> Result<StructList<'a>, Error> {
        let empty = StructList {
            msg: self.msg,
            seg: self.seg,
            first: 0,
            len: 0,
            data_len: 0,
            pointers: 0,
        };

        let Some(at) = self.slot(index) else {
            return Ok(empty);
        };
        let Some(raw) = self.msg.follow(self.seg, at)? else {
            return Ok(empty);
        };

        if raw.tag & 3 != LIST || (raw.tag >> 32) & 7 != COMPOSITE {
            return Err(bad("expected a composite list"));
        }

        // The first word of the list describes the elements.
        let words = (raw.tag >> 35) as usize;
        let element = self.msg.word(raw.seg, raw.start)?;
        let len = (element as u32 >> 2) as usize;
        let data_len = ((element >> 32) & 0xffff) as usize * WORD;
        let pointers = (element >> 48) as usize;
        let element_len = data_len + pointers * WORD;

        if element & 3 != STRUCT {
            return Err(bad("invalid list tag"));
        }

        // The elements must fit in the words the list pointer announces.
        let total = len
            .checked_mul(element_len)
            .ok_or_else(|| bad("size overflow"))?;
        if total > words * WORD {
            return Err(bad("list larger than its pointer says"));
        }

        self.msg.check(raw.seg, raw.start, WORD + words * WORD)?;

        Ok(StructList {
            msg: self.msg,
            seg: raw.seg,
            first: raw.start + WORD,
            len,
            data_len,
            pointers,
        })
    }
}

/// A list of structs.
#[derive(Clone, Copy)]
pub(crate) struct StructList<'a> {
    msg: &'a Message,
    seg: usize,
    first: usize,
    len: usize,
    data_len: usize,
    pointers: usize,
}

impl<'a> StructList<'a> {
    pub(crate) fn get(&self, index: usize) -> Option<Struct<'a>> {
        (index < self.len).then(|| Struct {
            msg: self.msg,
            seg: self.seg,
            start: self.first + index * (self.data_len + self.pointers * WORD),
            data_len: self.data_len,
            pointers: self.pointers,
        })
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = Struct<'a>> + '_ {
        (0..self.len).filter_map(|i| self.get(i))
    }
}

/// Where a pointer word lives in the message being built.
#[derive(Clone, Copy)]
pub(crate) struct Slot(usize);

/// A struct in the message being built.
#[derive(Clone, Copy)]
pub(crate) struct StructRef {
    start: usize,
    data_len: usize,
    pointers: usize,
}

impl StructRef {
    /// Pointer `index` of this struct.
    pub(crate) fn slot(&self, index: usize) -> Slot {
        assert!(index < self.pointers, "pointer index out of range");

        Slot(self.start + self.data_len + index * WORD)
    }
}

/// A list of structs in the message being built.
#[derive(Clone, Copy)]
pub(crate) struct ListRef {
    first: usize,
    len: usize,
    data_len: usize,
    pointers: usize,
}

impl ListRef {
    pub(crate) fn get(&self, index: usize) -> StructRef {
        assert!(index < self.len, "list index out of range");

        StructRef {
            start: self.first + index * (self.data_len + self.pointers * WORD),
            data_len: self.data_len,
            pointers: self.pointers,
        }
    }
}

/// Builds a one-segment message. The root pointer sits in the first word.
pub(crate) struct Builder {
    bytes: Vec<u8>,
}

impl Builder {
    pub(crate) fn new() -> Self {
        Self {
            bytes: vec![0; WORD],
        }
    }

    /// Reserves `words` zeroed words and returns their byte offset.
    fn alloc(&mut self, words: usize) -> usize {
        let at = self.bytes.len();
        self.bytes.resize(at + words * WORD, 0);

        at
    }

    /// Points the pointer word at `slot` to `target`. `info` holds the high
    /// 32 bits and the kind sits in the low two.
    fn point(&mut self, slot: Slot, target: usize, kind: u64, info: u64) {
        let offset = (target as i64 - (slot.0 as i64 + WORD as i64)) / WORD as i64;
        let word = ((offset as u32 as u64) << 2 & 0xffff_ffff) | kind | info << 32;

        self.bytes[slot.0..slot.0 + WORD].copy_from_slice(&word.to_le_bytes());
    }

    pub(crate) fn init_root(&mut self, data_words: usize, pointers: usize) -> StructRef {
        self.init_struct(Slot(0), data_words, pointers)
    }

    pub(crate) fn init_struct(
        &mut self,
        slot: Slot,
        data_words: usize,
        pointers: usize,
    ) -> StructRef {
        let start = self.alloc(data_words + pointers);
        let info = data_words as u64 | (pointers as u64) << 16;
        self.point(slot, start, STRUCT, info);

        StructRef {
            start,
            data_len: data_words * WORD,
            pointers,
        }
    }

    fn put(&mut self, s: StructRef, offset: usize, value: &[u8]) {
        assert!(offset + value.len() <= s.data_len, "field out of range");

        let at = s.start + offset;
        self.bytes[at..at + value.len()].copy_from_slice(value);
    }

    pub(crate) fn set_u8(&mut self, s: StructRef, offset: usize, value: u8) {
        self.put(s, offset, &[value]);
    }

    pub(crate) fn set_u16(&mut self, s: StructRef, offset: usize, value: u16) {
        self.put(s, offset, &value.to_le_bytes());
    }

    pub(crate) fn set_u32(&mut self, s: StructRef, offset: usize, value: u32) {
        self.put(s, offset, &value.to_le_bytes());
    }

    pub(crate) fn set_u64(&mut self, s: StructRef, offset: usize, value: u64) {
        self.put(s, offset, &value.to_le_bytes());
    }

    pub(crate) fn set_bool(&mut self, s: StructRef, bit: usize, value: bool) {
        assert!(bit / 8 < s.data_len, "field out of range");

        let at = s.start + bit / 8;
        let mask = 1 << (bit % 8);

        if value {
            self.bytes[at] |= mask;
        } else {
            self.bytes[at] &= !mask;
        }
    }

    /// Points `slot` to a byte list. An empty list stays a null pointer.
    fn set_bytes(&mut self, slot: Slot, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }

        let start = self.alloc(bytes.len().div_ceil(WORD));
        self.bytes[start..start + bytes.len()].copy_from_slice(bytes);
        self.point(slot, start, LIST, BYTES | (bytes.len() as u64) << 3);
    }

    pub(crate) fn set_text(&mut self, slot: Slot, text: &str) {
        // Text is a byte list that ends with a NUL. Empty text is a null pointer.
        if text.is_empty() {
            return;
        }

        let mut bytes = text.as_bytes().to_vec();
        bytes.push(0);
        self.set_bytes(slot, &bytes);
    }

    pub(crate) fn set_data(&mut self, slot: Slot, data: &[u8]) {
        self.set_bytes(slot, data);
    }

    pub(crate) fn init_struct_list(
        &mut self,
        slot: Slot,
        len: usize,
        data_words: usize,
        pointers: usize,
    ) -> ListRef {
        let words = len * (data_words + pointers);
        let tag_at = self.alloc(1 + words);

        // The tag word gives the element count and size.
        let tag = (len as u64) << 2 | (data_words as u64) << 32 | (pointers as u64) << 48;
        self.bytes[tag_at..tag_at + WORD].copy_from_slice(&tag.to_le_bytes());
        self.point(slot, tag_at, LIST, COMPOSITE | (words as u64) << 3);

        ListRef {
            first: tag_at + WORD,
            len,
            data_len: data_words * WORD,
            pointers,
        }
    }

    /// The root pointer, which names the first struct of the message.
    #[cfg(test)]
    pub(crate) fn root_slot(&self) -> Slot {
        Slot(0)
    }

    /// The framed message: a segment table, then the segment.
    pub(crate) fn finish(self) -> Vec<u8> {
        let words = (self.bytes.len() / WORD) as u32;
        let mut out = Vec::with_capacity(8 + self.bytes.len());

        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&words.to_le_bytes());
        out.extend_from_slice(&self.bytes);

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read(bytes: &[u8]) -> Result<Message, Error> {
        read_message(&mut &bytes[..]).await
    }

    #[tokio::test]
    async fn builds_and_reads_back() {
        let mut b = Builder::new();
        let root = b.init_root(1, 3);
        b.set_u32(root, 0, 0xdead_beef);
        b.set_bool(root, 33, true);
        b.set_text(root.slot(0), "hello");
        b.set_data(root.slot(1), &[1, 2, 3]);

        let list = b.init_struct_list(root.slot(2), 2, 0, 2);
        for (i, (k, v)) in [("a", "1"), ("bb", "22")].into_iter().enumerate() {
            b.set_text(list.get(i).slot(0), k);
            b.set_text(list.get(i).slot(1), v);
        }

        let message = read(&b.finish()).await.unwrap();
        let root = message.root().unwrap();

        assert_eq!(root.u32(0), 0xdead_beef);
        assert!(root.bool(33));
        assert!(!root.bool(32));
        assert_eq!(root.text(0).unwrap(), "hello");
        assert_eq!(root.data_bytes(1).unwrap(), [1, 2, 3]);

        let list = root.struct_list(2).unwrap();
        let pairs: Vec<_> = list
            .iter()
            .map(|e| (e.text(0).unwrap(), e.text(1).unwrap()))
            .collect();
        assert_eq!(pairs, [("a", "1"), ("bb", "22")]);
    }

    #[tokio::test]
    async fn fields_beyond_the_stored_struct_read_as_zero() {
        let mut b = Builder::new();
        let root = b.init_root(1, 1);
        b.set_u32(root, 0, 7);

        let message = read(&b.finish()).await.unwrap();
        let root = message.root().unwrap();

        assert_eq!(root.u32(0), 7);
        assert_eq!(root.u32(8), 0);
        assert_eq!(root.u16(64), 0);
        assert_eq!(root.text(5).unwrap(), "");
        assert!(root.structure(5).unwrap().is_none());
    }

    /// Two segments: the root pointer in segment 0 is a far pointer to a
    /// struct in segment 1.
    #[tokio::test]
    async fn follows_a_far_pointer() {
        let mut bytes = Vec::new();

        // Segment table: two segments of 1 and 3 words (padded to a word).
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());

        // Segment 0: far pointer to segment 1, landing pad at word 0.
        bytes.extend_from_slice(&(FAR | 1 << 32).to_le_bytes());

        // Segment 1: the landing pad (a struct pointer, offset 1, one data
        // word), a skipped word, then the struct's data word.
        bytes.extend_from_slice(&(STRUCT | 1 << 2 | 1 << 32).to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&42u64.to_le_bytes());

        // The pad is at word 0 of segment 1, so its offset 1 reaches word 2.
        let message = read(&bytes).await.unwrap();

        assert_eq!(message.root().unwrap().u32(0), 42);
    }

    #[tokio::test]
    async fn damaged_input_is_an_error_not_a_panic() {
        let mut b = Builder::new();
        let root = b.init_root(1, 2);
        b.set_text(root.slot(0), "some text");
        let list = b.init_struct_list(root.slot(1), 3, 1, 0);
        b.set_u32(list.get(2), 0, 9);
        let good = b.finish();

        // Every prefix, and every single-bit flip, must fail cleanly or read.
        for len in 0..good.len() {
            let _ = read(&good[..len]).await;
        }

        for bit in 0..good.len() * 8 {
            let mut bytes = good.clone();
            bytes[bit / 8] ^= 1 << (bit % 8);

            if let Ok(message) = read(&bytes).await {
                if let Ok(root) = message.root() {
                    let _ = root.text(0);
                    let _ = root.struct_list(1).map(|l| l.iter().count());
                }
            }
        }
    }

    #[tokio::test]
    async fn rejects_oversized_tables() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(MAX_SEGMENTS as u32).to_le_bytes());

        assert!(read(&bytes).await.is_err());
    }
}
