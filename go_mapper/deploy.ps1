$env:GOOS = "linux"
$env:GOARCH = "amd64"

go build -o action

docker build -t megamaxl/ow-mapper .
docker push megamaxl/ow-mapper

