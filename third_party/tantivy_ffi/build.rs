//! build.rs: 调 cbindgen 生成 C 头文件 paimon_tantivy_ffi.h
//!
//! 输出路径: $OUT_DIR/paimon_tantivy_ffi.h
//! Corrosion (CMake 侧) 会读 cargo metadata 里的 OUT_DIR,把头文件加入 C++ include path。

use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let header_path = out_dir.join("paimon_tantivy_ffi.h");

    let cfg = cbindgen::Config::from_file(PathBuf::from(&crate_dir).join("cbindgen.toml"))
        .expect("cbindgen.toml must exist at crate root");

    match cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(cfg)
        .generate()
    {
        Ok(bindings) => {
            bindings.write_to_file(&header_path);
            println!(
                "cargo:rerun-if-changed={}",
                PathBuf::from(&crate_dir).join("src").display()
            );
            println!("cargo:rerun-if-changed=cbindgen.toml");
            // 把头文件路径暴露给 Corrosion / 上游 CMake
            println!("cargo:include={}", out_dir.display());
            eprintln!("cbindgen: wrote {}", header_path.display());
        }
        Err(e) => {
            // cbindgen 失败不一定致命 (例如 CI 在没改 Rust 代码时跳过). 打 warning 继续。
            eprintln!("cbindgen generation failed: {e:?}");
        }
    }
}
