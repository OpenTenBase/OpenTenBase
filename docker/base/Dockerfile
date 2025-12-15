# Use a base image with necessary dependencies
FROM ubuntu:20.04

# Set timezone
RUN ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN apt-get update && \
    apt-get install -y \
        openssh-server 

# add user
RUN useradd -m -r -s /bin/bash opentenbase

RUN mkdir /data
RUN chown opentenbase /data -R
RUN chgrp opentenbase /data -R

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
EXPOSE 30004
CMD ["/usr/sbin/sshd", "-D"]

# Share belive configure
USER opentenbase
RUN mkdir /home/opentenbase/.ssh -p
RUN ssh-keygen -t rsa -f /home/opentenbase/.ssh/id_rsa -N ''
# Set right
RUN chown opentenbase /home/opentenbase/.ssh -R
RUN chgrp opentenbase /home/opentenbase/.ssh -R