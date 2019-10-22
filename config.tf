provider "aws" {
  profile = "personal"
  region  = "eu-west-2"
}

resource "aws_instance" "main-node" {
  ami           = "ami-00a1270ce1e007c27"
  instance_type = "t2.micro"
}
