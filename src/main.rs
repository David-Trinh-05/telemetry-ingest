mod ingest;

use std::{env, error::Error, future::Future, io};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let address = args.next().unwrap_or_else(|| "127.0.0.1:3000".into());
    if args.next().is_some() {
        return Err("usage: telemetry-ingest [host:port]".into());
    }

    let listener = TcpListener::bind(address).await?;
    let shutdown = shutdown_signal()?;
    let (app, worker) = ingest::pipeline(32);
    let writer = tokio::task::spawn_blocking(move || {
        let result = worker.run(io::stdout().lock());
        if let Err(error) = &result {
            eprintln!("telemetry sink failed: {error}");
        }
        result
    });

    eprintln!("listening on {}", listener.local_addr()?);
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await;
    writer.await??;
    server?;
    Ok(())
}

#[cfg(unix)]
fn shutdown_signal() -> io::Result<impl Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
    })
}

#[cfg(not(unix))]
fn shutdown_signal() -> io::Result<impl Future<Output = ()>> {
    Ok(async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            eprintln!("could not listen for Ctrl-C: {error}");
        }
    })
}
