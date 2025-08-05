use std::{
    env,
    fs::{self, File, OpenOptions, create_dir_all},
    io::{Write, read_to_string},
    path::{Path, PathBuf},
};

fn get_resolved_lua_content(path: &PathBuf) -> String {
    let file: File = OpenOptions::new()
        .read(true)
        .open(&path)
        .expect("provided path of lua file should exist");
    let content = read_to_string(file).unwrap();

    let mut new_content = String::with_capacity(content.len());
    for line in content.lines() {
        if line.trim().starts_with("--- @include ") {
            let filename: &str = line
                .split_whitespace()
                .skip(2)
                .next()
                .unwrap()
                .trim_matches('"');
            let filename = format!("{filename}.lua");
            let sub_path = path.parent().unwrap().join(&filename);

            let inplace: String = format!(
                "\n--- Resolved include {0}\n{1}\n--- End resolved include {0}\n\n",
                filename,
                get_resolved_lua_content(&sub_path)
            );
            new_content += inplace.as_str();
        } else {
            new_content += line;
            new_content += "\n";
        }
    }

    new_content
}

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let input_dir = Path::new("lua/commands");
    let output_dir = out_dir.join("lua");
    println!("Create directory {}.", output_dir.display());
    create_dir_all(&output_dir).unwrap();

    fs::create_dir_all(&output_dir).unwrap();

    for entry in fs::read_dir(input_dir).unwrap() {
        let entry = entry.unwrap();
        println!(
            "Processing and resolving imports of lua script {:?}",
            entry.path().display()
        );
        if entry.path().extension().and_then(|s| s.to_str()) != Some("lua") {
            continue;
        }
        let content = get_resolved_lua_content(&entry.path());
        let filename = entry.file_name();
        let out_path = output_dir.join(filename);
        println!(
            "Write result ({}B) to {}.",
            content.as_bytes().len(),
            out_path.display()
        );
        let mut outputfile = OpenOptions::new()
            .create(true)
            .write(true)
            .open(out_path)
            .unwrap();
        outputfile.write_all(content.as_bytes()).unwrap();
    }

    println!("cargo:rerun-if-changed=/build.rs");
    println!("cargo:rerun-if-changed=/lua");
}
