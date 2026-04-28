{
  description = "MongoDB Management Tools Environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
  };

  outputs = { self, nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };

      pythonEnv = pkgs.python312.withPackages (ps: [
        ps.pymongo
        ps.faker
      ]);
    in {
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          pkgs.mongosh
          pkgs.mongodb-atlas-cli
          pkgs.mongodb-tools
          pythonEnv
        ];

        shellHook = ''
          echo "🍃 MongoDB Tools Environment Ready!"
          echo "Commands: mongosh, atlas, mongoimport, mongorestore"
        '';
      };
    };
}
