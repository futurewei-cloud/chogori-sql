FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt upgrade -y
RUN apt install -y build-essential vim gdb strace psmisc pkg-config python3 python3-pip git sed
RUN pip3 install cmake
RUN apt install -y libreadline-dev bison flex libboost-dev uuid-dev librust-backtrace+libbacktrace-dev libboost-thread-dev libssl-dev

RUN cd /tmp && \
   git clone https://github.com/futurewei-cloud/chogori-seastar-rd.git && \
   cd chogori-seastar-rd && \
   ./install-dependencies.sh && \
   ./configure.py --mode=release && \
   ninja -C build/release && \
   ninja -C build/release install && \
   cd /tmp && \
   rm -rf /tmp/chogori-seastar-rd && \
   git clone https://github.com/futurewei-cloud/chogori-platform.git && \
   cd chogori-platform && \
   ./install_deps.sh && \
   mkdir -p build && \
   cd build && \
   cmake ../ && \
   make -j && \
   make install && \
   cd /tmp && \
   rm -rf /tmp/chogori-platform && \
   git clone --branch v0.4.0 https://github.com/google/glog.git && \
   cd glog && \
   mkdir -p build && \
   cd build && \
   cmake ../ && \
   make -j && \
   make install && \
   cd /tmp && \
   rm -rf /tmp/glog && \
   apt install -y libgflags-dev && \
   pip3 install psycopg2-binary && \
   cd

CMD ["/bin/bash"]
