use std::{path::PathBuf, process::ExitCode};

use clap::{Args, Parser, Subcommand};
use copperdb_adminimport::{
    export_neo4j_csv, import_offline, write_import_report, AdminImportError, ImportOptions,
    Neo4jCsvExportOptions,
};
use copperdb_storage::StorageEngine;
use copperdb_util::RequestCancellation;

type AdminResult<T> = Result<T, Box<AdminImportError>>;

#[derive(Debug, Parser)]
#[command(name = "copperdb-admin")]
#[command(about = "Offline administrative commands for copperdb")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Database {
        #[command(subcommand)]
        command: DatabaseCommand,
    },
}

#[derive(Debug, Subcommand)]
enum DatabaseCommand {
    Import {
        #[command(subcommand)]
        command: ImportCommand,
    },
    Export {
        #[command(subcommand)]
        command: ExportCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ImportCommand {
    Full(FullImportArgs),
}

#[derive(Debug, Subcommand)]
enum ExportCommand {
    Neo4jCsv(Neo4jCsvExportArgs),
}

#[derive(Debug, Args)]
struct FullImportArgs {
    #[arg(long)]
    database: String,
    #[arg(long)]
    target: PathBuf,
    #[arg(long)]
    data_dir: PathBuf,
    #[arg(long, required = true, num_args = 1..)]
    nodes: Vec<PathBuf>,
    #[arg(long)]
    relationships: Vec<PathBuf>,
    #[arg(long)]
    schema: Option<PathBuf>,
    #[arg(long)]
    report: Option<PathBuf>,
    #[arg(long, default_value_t = ',')]
    delimiter: char,
    #[arg(long, default_value_t = '"')]
    quote: char,
    #[arg(long, default_value_t = ';')]
    array_delimiter: char,
    #[arg(long, default_value_t = ';')]
    vector_delimiter: char,
    #[arg(long)]
    empty_strings_as_null: bool,
    #[arg(long, default_value_t = 0)]
    bad_relationship_tolerance: usize,
    #[arg(long)]
    skip_bad_relationships: bool,
    #[arg(long, default_value_t = 1_000)]
    chunk_size: usize,
}

#[derive(Debug, Args)]
struct Neo4jCsvExportArgs {
    #[arg(long)]
    source: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = ',')]
    delimiter: char,
    #[arg(long, default_value_t = '"')]
    quote: char,
    #[arg(long, default_value_t = ';')]
    array_delimiter: char,
    #[arg(long, default_value_t = ';')]
    vector_delimiter: char,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(error.exit_code() as u8)
        }
    }
}

fn run(cli: Cli) -> AdminResult<()> {
    match cli.command {
        Command::Database {
            command:
                DatabaseCommand::Import {
                    command: ImportCommand::Full(args),
                },
        } => {
            let target = args.target.clone();
            let options = options_from_args(args)?;
            let cancellation = RequestCancellation::new();
            let report = import_offline(&target, &options, &cancellation)?;
            write_import_report(&options, &report)?;
            println!(
                "imported {} nodes and {} relationships into {}",
                report.nodes_imported, report.relationships_imported, report.database_name
            );
            Ok(())
        }
        Command::Database {
            command:
                DatabaseCommand::Export {
                    command: ExportCommand::Neo4jCsv(args),
                },
        } => {
            let source = args.source.clone();
            let options = export_options_from_args(args)?;
            let engine = StorageEngine::open(source).map_err(AdminImportError::from)?;
            let cancellation = RequestCancellation::new();
            let report = export_neo4j_csv(&engine, &options, &cancellation)?;
            println!(
                "exported {} nodes and {} relationships into {}",
                report.nodes_exported,
                report.relationships_exported,
                options.output_directory.display()
            );
            Ok(())
        }
    }
}

fn options_from_args(args: FullImportArgs) -> AdminResult<ImportOptions> {
    Ok(ImportOptions {
        database_name: args.database,
        node_sources: args.nodes,
        relationship_sources: args.relationships,
        schema_file: args.schema,
        data_directory: args.data_dir,
        report_file: args.report,
        delimiter: ascii_byte(args.delimiter)?,
        quote: ascii_byte(args.quote)?,
        array_delimiter: args.array_delimiter,
        vector_delimiter: args.vector_delimiter,
        empty_strings_as_null: args.empty_strings_as_null,
        bad_relationship_tolerance: args.bad_relationship_tolerance,
        skip_bad_relationships: args.skip_bad_relationships,
        chunk_size: args.chunk_size,
    })
}

fn export_options_from_args(args: Neo4jCsvExportArgs) -> AdminResult<Neo4jCsvExportOptions> {
    Ok(Neo4jCsvExportOptions {
        output_directory: args.output,
        delimiter: ascii_byte(args.delimiter)?,
        quote: ascii_byte(args.quote)?,
        array_delimiter: args.array_delimiter,
        vector_delimiter: args.vector_delimiter,
    })
}

fn ascii_byte(value: char) -> AdminResult<u8> {
    u8::try_from(value).map_err(|_| {
        AdminImportError::UnsupportedSourceFormat {
            path: PathBuf::from(format!("non-ASCII CSV delimiter {value:?}")),
        }
        .into()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_cli_import_options() {
        let options = options_from_args(FullImportArgs {
            database: "northwind".into(),
            target: PathBuf::from("ignored"),
            data_dir: PathBuf::from("data"),
            nodes: vec![PathBuf::from("nodes.csv")],
            relationships: vec![PathBuf::from("relationships.csv")],
            schema: Some(PathBuf::from("copperdb-schema.json")),
            report: Some(PathBuf::from("report.json")),
            delimiter: ',',
            quote: '"',
            array_delimiter: ';',
            vector_delimiter: ';',
            empty_strings_as_null: true,
            bad_relationship_tolerance: 3,
            skip_bad_relationships: true,
            chunk_size: 42,
        })
        .unwrap();
        assert_eq!(options.database_name, "northwind");
        assert_eq!(options.delimiter, b',');
        assert_eq!(
            options.schema_file,
            Some(PathBuf::from("copperdb-schema.json"))
        );
        assert!(options.empty_strings_as_null);
        assert_eq!(options.bad_relationship_tolerance, 3);
        assert!(options.skip_bad_relationships);
        assert_eq!(options.chunk_size, 42);
    }

    #[test]
    fn converts_cli_export_options() {
        let options = export_options_from_args(Neo4jCsvExportArgs {
            source: PathBuf::from("database"),
            output: PathBuf::from("export"),
            delimiter: ';',
            quote: '\'',
            array_delimiter: '|',
            vector_delimiter: ':',
        })
        .unwrap();
        assert_eq!(options.output_directory, PathBuf::from("export"));
        assert_eq!(options.delimiter, b';');
        assert_eq!(options.quote, b'\'');
        assert_eq!(options.array_delimiter, '|');
        assert_eq!(options.vector_delimiter, ':');
    }
}
