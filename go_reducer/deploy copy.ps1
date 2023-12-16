$env:GOOS = "linux"
$env:GOARCH = "amd64"

go build -o action

docker build -t megamaxl/ow-reducer .
docker push megamaxl/ow-reducer

