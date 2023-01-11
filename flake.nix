{
	inputs = {
		nixpkgs.url = github:nixos/nixpkgs;
		debBundler = {
			url = github:juliosueiras-nix/nix-utils;
			inputs.nixpkgs.follows = "nixpkgs";
		};
	};

	outputs = { nixpkgs, debBundler, ... }: with nixpkgs.lib; {
		packages = genAttrs [
			"x86_64-linux"
		] (system: let
			pkgs = import nixpkgs { inherit system; };
		in rec {
			yarn-cgroup-exporter = pkgs.callPackage ./. {};
			default = yarn-cgroup-exporter;
			deb = debBundler.bundlers.deb {
				inherit system;
				program = "${default}/bin/${default.pname}";
			};
		});
	};
}
