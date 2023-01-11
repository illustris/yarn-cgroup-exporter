{
	inputs.nixpkgs.url = github:nixos/nixpkgs;

	outputs = { nixpkgs, ... }: with nixpkgs.lib; {
		packages = genAttrs [
			"x86_64-linux"
		] (system: let
			pkgs = import nixpkgs { inherit system; };
		in rec {
			yarn-cgroup-exporter = pkgs.callPackage ./. {};
			default = yarn-cgroup-exporter;
		});
	};
}
