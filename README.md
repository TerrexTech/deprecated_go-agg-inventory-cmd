# go-agg-inventory-cmd
Mongo aggregate with Custom time - need to divide into cmd and query

#### Steps to run mongo
---
* Install vagrant - make sure its on 2.1.5 or latest
* Then run
    * vagrant plugin install vagrant-docker-compose
    * vagrant plugin install vagrant-vbguest
* Run vagrant up
* Type the following command - to forward port
    * ssh -i .vagrant/machines/default/virtualbox/private_key -p 2222 vagrant@localhost -L 27017:0.0.0.0:27017
* If it asks for a password - default is vagrant
* Here are the docker commands to get to mongo and check if everything is fine
    * docker exec -it container bash
    * mongo -u root -p root admin
    * show databases
    * show collections
    * db.users.find()
    * db.dropDatabase()

### After that run go-agg-inventory-cmd
* Look at main file --- the router.HandleFunc tell you routes that I am using.
* You can connect your front-end to this by using the URL: localhost:8080/name of route