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
    db: String,
}

fn main() {
    let args = Args::parse();
    let fasta = args.fasta;
    let db = args.db;

    let mut reader = fasta::Reader::from_file(fasta).unwrap();

    let mut connection = get_connection(&db);
    run_migrations(&mut connection);

    for result in reader.records() {
        let record = result.expect("Error during fasta record parsing");
        let mut bases = vec![];
        for base in record.seq() {
            let bp = *base as char;
            bases.push(bp.to_string());
        }
        let nodes = models::Node::bulk_create(&mut connection, &bases);
        let mut edges = vec![];
        for i in (0..nodes.len()-1) {
            edges.push(models::Edge{id: 0, source_id: nodes[i].id, target_id: nodes[i+1].id});
        }
        models::Edge::bulk_create(&mut connection, &edges);
        // let inserted_nodes : QueryResult<Vec<models::Node>> = diesel::insert_into(schema::nodes::table).values(&nodes).returning(schema::nodes::all_columns).get_results(connection);
        // println!("{:?}", inserted_nodes);
        // let source = nodes[0];
        //     target = Some(diesel::insert_into(schema::nodes::table).values(&models::NewNode{ base: bp.to_string().deref() }).returning(schema::nodes::all_columns).get_result(connection).unwrap());
        //     if (!source.is_none() && !target.is_none()) {
        //         diesel::insert_into(schema::edges::table).values(&models::NewEdge{source_id: source.unwrap().id, target_id: target.as_ref().unwrap().id}).execute(connection);
        //     }
        //     source = target;
        // }
    }

    println!("Hello, world!");
}
