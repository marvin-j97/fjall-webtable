mod webtable;
mod wide_column;

use fjall::Config;
use webtable::Webtable;

fn main() -> anyhow::Result<()> {
    let keyspace = Config::default().temporary(true).open()?;
    let webtable = Webtable::new(keyspace)?;

    for url in ["https://vedur.is", "https://news.ycombinator.com"] {
        eprintln!("Scraping {url:?}");
        let res = reqwest::blocking::get(url)?;

        if res.status().is_success() {
            let html = res.text()?;
            webtable.insert(url, &html)?;
        }
    }

    eprintln!("-- Scanning metadata --");

    for cell in webtable.iter_metadata() {
        let cell = cell?;
        eprintln!("{cell:?}");
    }

    eprintln!("-- Scanning anchors --");

    for cell in webtable.iter_anchors_to_page("") {
        let cell = cell?;
        eprintln!("{cell:?}");
    }

    Ok(())
}
