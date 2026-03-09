{
  description = "duck-lk - DuckDB extension for LabKey LIMS";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rustToolchain = pkgs.rust-bin.stable."1.90.0".default.override {
          extensions = [
            "rust-src"
            "rust-analyzer"
          ];
        };

        buildInputs =
          with pkgs;
          [
            openssl
          ]
          ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            libiconv
            apple-sdk_15
          ];

        nativeBuildInputs = with pkgs; [
          pkg-config
        ];
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs =
            buildInputs
            ++ (with pkgs; [
              rustToolchain
              cargo-nextest
              git
              gnumake
              python313
              duckdb
              direnv
            ]);

          inherit nativeBuildInputs;

          shellHook = ''
            echo "duck-lk development environment"
            echo "Rust: $(rustc --version)"
            echo "DuckDB: $(duckdb --version)"
          '';

          RUST_BACKTRACE = 1;
          OPENSSL_NO_VENDOR = "1";
          OPENSSL_DIR = "${pkgs.openssl.dev}";
          OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
          OPENSSL_INCLUDE_DIR = "${pkgs.openssl.dev}/include";
        };
      }
    );
}
