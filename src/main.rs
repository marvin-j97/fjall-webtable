mod webtable;
mod wide_column;

use fjall::Config;
use webtable::Webtable;

fn main() -> fjall::Result<()> {
    let keyspace = Config::default().temporary(true).open()?;
    let webtable = Webtable::new(keyspace)?;

    for url in ["https://vedur.is", "https://news.ycombinator.com"] {
        eprintln!("Scraping {url:?}");
        let res = reqwest::blocking::get(url).unwrap();

        if res.status().is_success() {
            let html = res.text().unwrap();
            webtable.insert(url, &html)?;
        }
    }

    eprintln!("Scanning database");

    for cell in webtable.iter_metadata() {
        let cell = cell?;
        eprintln!("{cell:?}");
    }

    for cell in webtable.iter_anchors_to_page("org.duckdb") {
        let cell = cell?;
        eprintln!("{cell:?}");
    }

    Ok(())
}
