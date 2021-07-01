# Building the image
``` sh
docker build -t "chogori-builder-sql" .
```

# Building and installing Chogori-Seastar-RD
## Run the container from the image we built above
This mounts your host current directory `PWD` into the `/build` directory of the container. Any changes you do in the container inside the `/host` direc
tory are reflected into the host (much like a symlink)
``` sh
docker run -it --rm --init -v ${PWD}:/build chogori-builder-sql

## And now run the sql build and installation steps inside this container
cd /build
git clone https://github.com/futurewei-cloud/chogori-sql.git
cd chogori-sql

``` sh
mkdir -p build
cd build
cmake ../
make -j




