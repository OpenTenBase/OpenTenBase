# Use a base image with necessary dependencies
FROM ubuntu:20.04

# Set the timezone
RUN ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
# Install required packages
RUN apt-get update && \
    apt-get install -y \
        git \
        build-essential \
        libssl-dev \
        uuid-dev \
        gcc \
        make \
        libreadline-dev \
        zlib1g-dev \
        libssl-dev \
        libossp-uuid-dev \
        bison \
        flex \
        vim \
        openssh-server \
        iputils-ping # ping command

# Clone the source code repository
RUN git clone https://github.com/OpenTenBase/OpenTenBase /data/opentenbase/OpenTenBase

# Set environment variables
ENV SOURCECODE_PATH=/data/opentenbase/OpenTenBase
ENV INSTALL_PATH=/data/opentenbase/install

COPY ./copy-ssh-keys /usr/bin

# Navigate to the source code directory
WORKDIR $SOURCECODE_PATH

# Remove existing installation directory
RUN rm -rf $INSTALL_PATH/opentenbase_bin_v2.0

# Configure and build the application
RUN ./configure --prefix=$INSTALL_PATH/opentenbase_bin_v2.0 --enable-user-switch --with-openssl --with-ossp-uuid CFLAGS=-g && \
    make clean && \
    make -sj && \
    make install

# Make necessary files executable
RUN chmod +x $SOURCECODE_PATH/contrib/pgxc_ctl/make_signature

# Navigate to the contrib directory
WORKDIR $SOURCECODE_PATH/contrib

# Build and install additional components
RUN make -sj && \
    make install

# Add opentenbase user
RUN useradd -m -r -s /bin/bash opentenbase

RUN echo "export OPENTENBASE_HOME=/data/opentenbase/install/opentenbase_bin_v2.0 \n\
export PATH=/data/opentenbase/install/opentenbase_bin_v2.0/bin:$PATH \n\
export LD_LIBRARY_PATH=$OPENTENBASE_HOME/lib:${LD_LIBRARY_PATH} \n\
export LC_ALL=C \n\
" >> /home/opentenbase/.bashrc 

RUN echo "PATH=\"/data/opentenbase/install/opentenbase_bin_v2.0/bin:$PATH\"" >> /etc/environment 

# Set root password for SSH access
RUN echo 'root:qwerty' | chpasswd
RUN echo 'opentenbase:qwerty' | chpasswd
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN mkdir /var/run/sshd
EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]

# Share belive config
RUN su opentenbase
RUN mkdir /home/opentenbase/.ssh -p
RUN ssh-keygen -t rsa -f /home/opentenbase/.ssh/id_rsa -N ''
# Set right
RUN chown opentenbase /home/opentenbase/.ssh -R
RUN chgrp opentenbase /home/opentenbase/.ssh -R
RUN chown opentenbase /data -R
RUN chgrp opentenbase /data -R
