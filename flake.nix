{
  description = "Subprocess container: process-isolated ModuleContainer implementation for the Logos module runtime";

  inputs = {
    logos-nix.url = "github:logos-co/logos-nix";
    nixpkgs.follows = "logos-nix/nixpkgs";
    logos-container.url = "github:logos-co/logos-container";
    # Without this, logos-container resolves its OWN pinned logos-nix, so
    # overriding logos-nix here (as the workspace and the Windows work both do)
    # silently would not reach it.
    logos-container.inputs.logos-nix.follows = "logos-nix";
  };

  outputs = { self, nixpkgs, logos-nix, logos-container }:
    let
      systems = [ "aarch64-darwin" "x86_64-darwin" "aarch64-linux" "x86_64-linux" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f {
        inherit system;
        pkgs = import nixpkgs { inherit system; };
        logosContainer = logos-container.packages.${system}.default;
      });

      # Same, plus the "x86_64-windows" pseudo-system. Not logos-nix's shared
      # forAllTargets, because this flake also threads logosContainer through
      # -- and that dependency follows the TARGET (it is a header-only
      # contract compiled into this library, not a tool run at build time).
      forAllTargets = f:
        nixpkgs.lib.genAttrs (systems ++ [ "x86_64-windows" ]) (system: f {
          inherit system;
          pkgs =
            if system == "x86_64-windows"
            then logos-nix.lib.mkWindowsPkgs { buildSystem = "x86_64-linux"; }
            else import nixpkgs { inherit system; };
          logosContainer = logos-container.packages.${system}.default;
        });
    in
    {
      packages = forAllTargets ({ pkgs, system, logosContainer, ... }:
        let
          common = import ./nix/default.nix { inherit pkgs logosContainer; };
          src = ./.;

          build = import ./nix/build.nix { inherit pkgs common src; };

          lib = import ./nix/lib.nix { inherit pkgs common build; };
          include = import ./nix/include.nix { inherit pkgs common src; };
          tests = import ./nix/tests.nix { inherit pkgs common build logosContainer; };

          logos-container-subprocess = pkgs.symlinkJoin {
            name = "logos-container-subprocess";
            paths = [ lib include ];
          };
        in
        {
          logos-container-subprocess-lib = lib;
          logos-container-subprocess-include = include;
          logos-container-subprocess-tests = tests;

          logos-container-subprocess = logos-container-subprocess;

          default = logos-container-subprocess;
        }
      );

      checks = forAllSystems ({ pkgs, system, ... }:
        let
          testsPkg = self.packages.${system}.logos-container-subprocess-tests;
        in
        {
          tests = pkgs.runCommand "logos-container-subprocess-tests"
            {
              nativeBuildInputs = [ testsPkg ];
            } ''
            echo "Running logos-container-subprocess tests..."
            ${testsPkg}/bin/logos_container_subprocess_tests
            mkdir -p $out
            touch $out/.tests-passed
          '';
        }
      );

      devShells = forAllSystems ({ pkgs, ... }: {
        default = pkgs.mkShell {
          nativeBuildInputs = [
            pkgs.cmake
            pkgs.ninja
            pkgs.pkg-config
          ];
          buildInputs = [
            pkgs.boost
            pkgs.nlohmann_json
            pkgs.spdlog
            pkgs.gtest
          ];
        };
      });
    };
}
