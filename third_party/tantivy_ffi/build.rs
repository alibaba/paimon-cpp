//! build.rs: runs cbindgen to generate the C header paimon_tantivy_ffi.h.
//!
//! Output path: $OUT_DIR/paimon_tantivy_ffi.h
//! Corrosion (on the CMake side) reads OUT_DIR from cargo metadata and adds the
//! header to the C++ include path.

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
            // Expose the header directory to Corrosion / the upstream CMake build.
            println!("cargo:include={}", out_dir.display());
            eprintln!("cbindgen: wrote {}", header_path.display());
        }
        Err(e) => {
            // cbindgen failure is not necessarily fatal (e.g. CI skips it when the
            // Rust code is unchanged); log a warning and continue.
            eprintln!("cbindgen generation failed: {e:?}");
        }
    }
}
