use super::wide_column::{Cell, WideColumnDb};
use fjall::Keyspace;
use visdom::{Vis, types::Elements};

fn reverse_domain_key(url: &str) -> String {
    let mut components: Vec<_> = url.split(".").collect();
    components.reverse();
    components.join(".")
}

pub struct Webtable {
    #[allow(unused)]
    keyspace: Keyspace,

    inner: WideColumnDb,
    lg_meta: WideColumnDb,
    lg_contents: WideColumnDb,
}

impl Webtable {
    pub fn new(keyspace: Keyspace) -> fjall::Result<Self> {
        let inner = WideColumnDb::new(keyspace.clone(), "webtable")?;
        let lg_meta = WideColumnDb::new(keyspace.clone(), "lg_meta")?;
        let lg_contents = WideColumnDb::new(keyspace.clone(), "lg_contents")?;

        Ok(Self {
            keyspace,
            inner,
            lg_meta,
            lg_contents,
        })
    }

    fn parse_anchors<'a>(root: &Elements<'a>) -> Elements<'a> {
        let anchors = root.find("a");
        anchors
    }

    pub fn iter_primary(&self) -> impl Iterator<Item = fjall::Result<Cell>> {
        self.inner.prefix("")
    }

    pub fn iter_metadata(&self) -> impl Iterator<Item = fjall::Result<Cell>> {
        self.lg_meta.prefix("")
    }

    pub fn iter_anchors_to_page(
        &self,
        rev_domain: &str,
    ) -> impl Iterator<Item = fjall::Result<Cell>> {
        let prefix = format!("{rev_domain}\0anchor\0");
        self.inner.prefix(prefix)
    }

    pub fn insert(&self, url: &str, html: &str) -> fjall::Result<()> {
        let rev_url = reverse_domain_key(url.trim_start_matches("https://"));

        let unix_timestamp = std::time::SystemTime::UNIX_EPOCH
            .elapsed()
            .unwrap()
            .as_secs();

        let root = Vis::load(html).unwrap();

        if let Some(lang) = root.find("html").attr("lang") {
            let lang = lang.to_string().to_uppercase();

            self.lg_meta
                .insert(&rev_url, "language", "", None, lang.as_bytes())?;
        }

        self.lg_contents.insert(
            &rev_url,
            "contents",
            "",
            Some(unix_timestamp),
            html.as_bytes(),
        )?;

        self.lg_meta.insert(
            &rev_url,
            "checksum",
            "",
            Some(unix_timestamp),
            md5::compute(&html).as_slice(),
        )?;

        for anchor in Self::parse_anchors(&root) {
            let href = anchor.get_attribute("href");
            let href = href.unwrap().to_string();

            if href.starts_with("mailto:") || href.starts_with("tel:") || href.starts_with("#") {
                continue;
            }

            let href = if href.starts_with("/") {
                format!("{url}{href}")
            } else if href.starts_with("http") {
                href
            } else {
                format!("{url}/{href}")
            };

            let href = href.trim_start_matches("https://");
            let href = href.trim_start_matches("http://");

            let mut splits = href.split('/');
            let domain = splits.next().unwrap();
            // let _pathname = format!("/{}", splits.next().unwrap());
            let rev_domain = reverse_domain_key(domain);

            let text = anchor.text();

            self.inner
                .insert(&rev_domain, "anchor", url, None, text.as_bytes())?;
        }

        Ok(())
    }
}
