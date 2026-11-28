//! The driver's binary: parse the command line and run it.

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    pqbench_driver::run().await
}
