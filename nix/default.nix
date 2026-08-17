# Common build configuration shared across all packages
{ pkgs, logosContainer }:

{
  pname = "logos-container-subprocess";
  version = "0.1.0";

  nativeBuildInputs = [
    pkgs.cmake
    pkgs.ninja
    pkgs.pkg-config
  ];

  # Boost (Process/Asio) drives the child process and the stdin token pipe;
  # spdlog is the logger; nlohmann backs the contract's descriptor types;
  # logosContainer provides the ModuleContainer interface header. No Qt — the
  # child side is just a stdin read, handled by the host binary in liblogos.
  buildInputs = [
    pkgs.boost
    pkgs.nlohmann_json
    pkgs.spdlog
    pkgs.gtest
    logosContainer
  ];

  cmakeFlags = [
    "-GNinja"
    "-DLOGOS_CONTAINER_ROOT=${logosContainer}"
  ];

  env = {
    LOGOS_CONTAINER_ROOT = "${logosContainer}";
  };

  meta = with pkgs.lib; {
    description = "Subprocess container: process-isolated ModuleContainer implementation for the Logos module runtime";
    platforms = platforms.unix ++ platforms.windows;
  };
}
