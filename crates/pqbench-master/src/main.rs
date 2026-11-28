//! The master's binary: parse the command line and run it.

#[tokio::main]
async fn main() -> std::process::ExitCode {
    pqbench_master::run().await
}
