#! /bin/sh

cd $GOPATH/src/cloud.google.com/go/ && git checkout master && git pull
cd $GOPATH/src/github.com/araddon/gou && git checkout master && git pull
cd $GOPATH/src/github.com/Azure/azure-sdk-for-go && git checkout master && git pull
cd $GOPATH/src/github.com/aws/aws-sdk-go && git checkout master && git pull
cd $GOPATH/src/github.com/golang/protobuf && git checkout master && git pull
cd $GOPATH/src/github.com/googleapis/gax-go && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/logrus && git checkout master && git pull
cd $GOPATH/src/github.com/pborman/uuid && git checkout master && git pull
cd $GOPATH/src/golang.org/pkg/sftp && git checkout master && git pull
cd $GOPATH/src/golang.org/x/crypto && git checkout master && git pull
cd $GOPATH/src/golang.org/x/net && git checkout master && git pull
cd $GOPATH/src/golang.org/x/oauth2 && git checkout master && git pull
cd $GOPATH/src/google.golang.org/api && git checkout master && git pull
cd $GOPATH/src/google.golang.org/grpc && git checkout master && git pull
cd $GOPATH/src/google.golang.org/genproto && git checkout master && git pull

