{stdenv, cmake, rdkafka, curl, ...}:
stdenv.mkDerivation {
	pname = "yarn-cgroup-exporter";
	version = "1.0";
	src = ./src;
	nativeBuildInputs = [ cmake ];
	buildInputs = [ curl rdkafka ];
}
