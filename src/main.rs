use bio::io::fasta;
use clap::Parser;
use gen::get_connection;
use gen::migrations::run_migrations;
use gen::models;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    fasta: String,
    name: String,
    db: String,
}

fn main() {
    let args = Args::parse();
    let fasta = args.fasta;
    let name = args.name;
    let db = args.db;

    let mut reader = fasta::Reader::from_file(fasta).unwrap();

    let mut conn = get_connection(&db);
    run_migrations(&mut conn);

    if !models::Collection::exists(&mut conn, &name) {
        let collection = models::Collection::create(&mut conn, &name);

        for result in reader.records() {
            let record = result.expect("Error during fasta record parsing");
            let seq_id = models::Sequence::create(
                &mut conn,
                collection.id,
                "DNA".to_string(),
                record.id().to_string(),
                &String::from_utf8(record.seq().to_vec()).unwrap(),
                false,
            );
            let sc_id = models::SequenceCollection::create(&mut conn, collection.id, seq_id);
        }
        println!("Created it");
    } else {
        println!("Collection {:1} already exists", name);
    }
}
