use std::{io::Write, os::unix::io::AsRawFd, sync::Arc};

use auring::Uring;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut f = tempfile::NamedTempFile::new()?;
    let fd = f.as_raw_fd();

    let ring = Arc::new(Uring::new(128)?);
    f.write_all("hello, world".as_ref())?;

    let mut handles = vec![];
    for _i in 0..32 {
        let ring = Arc::clone(&ring);
        handles.push(tokio::spawn(async move {
            for _j in 0..128 {
                let buf = vec![0; 128];
                ring.read(fd, buf, 0).unwrap().await.unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    Ok(())
}
