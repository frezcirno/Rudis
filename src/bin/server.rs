use libc::srand;
use rudis::{config::Config, server::Server};
use std::time;
use tokio;

/// set proc title
fn set_proc_title(title: &str) {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;

        unsafe {
            let title = CString::new(title).unwrap();
            libc::prctl(libc::PR_SET_NAME, title.as_ptr() as libc::c_ulong, 0, 0, 0);
        }
    }
}

/// set locale
fn set_locale() {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;

        unsafe {
            let locale = CString::new("C").unwrap();
            libc::setlocale(libc::LC_COLLATE, locale.as_ptr());
        }
    }
}

async fn amain() {
    // print cwd
    let cwd = std::env::current_dir().unwrap();
    log::info!("cwd: {:?}", cwd);

    

    let config = Config::from_toml("./rudis.toml");
    let server = Server::from_config(config).await;
    let _ = server.start().await;
}

fn main() {
    env_logger::init();

    set_proc_title("rudis");

    set_locale();

    unsafe {
        srand(
            time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32,
        );
    }

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
        .block_on(amain());
}
