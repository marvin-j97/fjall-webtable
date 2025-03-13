use byteview::ByteView;
use fjall::{Keyspace, KvPair, Partition, Slice};
use self_cell::self_cell;

pub type Timestamp = u64;

pub struct WideColumnTable {
    #[allow(unused)]
    keyspace: Keyspace,

    primary: Partition,
}

fn serialize_cell_key(
    row_key: &str,
    col_family: &str,
    col_qual: &str,
    ts: Option<Timestamp>,
) -> Slice {
    let mut key = ByteView::with_size(
        row_key.len()
            + 1
            + col_family.len()
            + 1
            + col_qual.len()
            + 1
            + std::mem::size_of::<Timestamp>(),
    );

    {
        let mut buf = key.get_mut().unwrap();
        let mut offset = 0;

        // Copy row_key
        buf[offset..offset + row_key.len()].copy_from_slice(row_key.as_bytes());
        offset += row_key.len();
        buf[offset] = b'\0';
        offset += 1;

        // Copy col_family
        buf[offset..offset + col_family.len()].copy_from_slice(col_family.as_bytes());
        offset += col_family.len();
        buf[offset] = b'\0';
        offset += 1;

        // Copy col_key
        buf[offset..offset + col_qual.len()].copy_from_slice(col_qual.as_bytes());
        offset += col_qual.len();
        buf[offset] = b'\0';
        offset += 1;

        buf[offset..offset + std::mem::size_of::<Timestamp>()]
            .copy_from_slice(&(!ts.unwrap_or_default()).to_be_bytes());
    }

    Slice::from(key)
}

struct CellInner<'a> {
    row_key: &'a str,
    column_family: &'a str,
    column_qualifier: &'a str,
    timestamp: Timestamp,
    value: &'a [u8],
}

self_cell!(
    pub struct Cell {
        owner: KvPair,

        #[covariant]
        dependent: CellInner,
    }
);

impl std::fmt::Debug for Cell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}@{}:{}?{} => {:?}",
            self.row_key(),
            self.column_family(),
            self.column_qualifier(),
            self.timestamp(),
            if self.value().len() > 50 {
                String::from_utf8_lossy(self.value().get(0..50).unwrap())
            } else {
                String::from_utf8_lossy(self.value())
            },
        )
    }
}

impl Cell {
    pub fn value(&self) -> &[u8] {
        &self.borrow_dependent().value
    }

    pub fn timestamp(&self) -> Timestamp {
        self.borrow_dependent().timestamp
    }

    pub fn column_family(&self) -> &str {
        &self.borrow_dependent().column_family
    }

    pub fn column_qualifier(&self) -> &str {
        &self.borrow_dependent().column_qualifier
    }

    pub fn row_key(&self) -> &str {
        self.borrow_dependent().row_key
    }
}

impl WideColumnTable {
    pub fn new(keyspace: Keyspace, name: &str) -> fjall::Result<Self> {
        let primary = keyspace.open_partition(name, Default::default())?;
        Ok(Self { keyspace, primary })
    }

    pub fn insert(
        &self,
        row_key: &str,
        col_family: &str,
        col_qual: &str,
        ts: Option<Timestamp>,
        value: &[u8],
    ) -> fjall::Result<()> {
        let cell_key = serialize_cell_key(row_key, col_family, col_qual, ts);

        self.primary.insert(cell_key, value)
    }

    pub fn prefix(
        &self,
        prefix: impl Into<fjall::Slice>,
    ) -> impl Iterator<Item = fjall::Result<Cell>> {
        self.primary.prefix(prefix.into()).map(|kv| {
            Ok(Cell::new(kv?, |(k, v)| {
                let mut splits = k.split(|&x| x == b'\0');
                let row_key = std::str::from_utf8(splits.next().unwrap()).unwrap();
                let column_family = std::str::from_utf8(splits.next().unwrap()).unwrap();
                let column_qualifier = std::str::from_utf8(splits.next().unwrap()).unwrap();
                let ts_bytes = splits.next().unwrap();
                debug_assert_eq!(std::mem::size_of::<Timestamp>(), ts_bytes.len());

                let mut buf = [0; std::mem::size_of::<Timestamp>()];
                buf.copy_from_slice(ts_bytes);
                let timestamp = !Timestamp::from_be_bytes(buf);

                CellInner {
                    row_key,
                    column_family,
                    column_qualifier,
                    timestamp,
                    value: &v,
                }
            }))
        })
    }
}
