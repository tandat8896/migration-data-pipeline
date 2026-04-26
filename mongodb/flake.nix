{
  description = "MongoDB Management Tools Environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
  };

  outputs = { self, nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
    in {
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          pkgs.mongosh
          pkgs.mongodb-atlas-cli
          pkgs.mongodb-tools
        ];

        shellHook = ''
          echo "🍃 MongoDB Tools Environment Ready!"
          echo "Commands: mongosh, atlas, mongoimport, mongorestore"
        '';
      };
    };
}
