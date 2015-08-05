var express = require('express');
var bodyParser = require('body-parser');
var cassandra = require('cassandra-driver');
var async = require('async');
var path = require('path');

var Long = require('cassandra-driver').types.Long;
var cfenv = require("cfenv");
var appEnv = cfenv.getAppEnv();
var cassandra_ob=appEnv.getService("accountCassandra");

var authProvider=new cassandra.auth.PlainTextAuthProvider(cassandra_ob.credentials["username"],cassandra_ob.credentials["password"] );

var keyspace_name=cassandra_ob.credentials["keyspace_name"];
//var keyspace_name="test";

var insertAccountInfo='INSERT INTO '+keyspace_name+'.account (account_number ,customer_type,sub_status,plan ,first_name  ,last_name  ,email  ,birth_date  ,account_type  ,address_line1  ,city_name ,state_code ,postal_code ,postal_code_extention ,latitude ,longitude ,SIM ,IMSI  ,PUK  ,PUK2  ,card_category  , card_type  , security_code , expiration_date ,card_number ) '
+'values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);';
var getAccountById = 'SELECT * FROM '+keyspace_name+'.account WHERE account_number = ?;';
var app = express();
var updateAccountInfo='update '+keyspace_name+'.account set address_line1=?, city_name=?, state_code=?, postal_code=?, email=? WHERE account_number=?;';

/*// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');


*/
app.use(bodyParser.json());

app.use(bodyParser.urlencoded({ extended: false }));

app.use(express.static(path.join(__dirname, 'public')));

//app.use('/', order_details_route);
//app.use('/items', items);


var client = new cassandra.Client( { authProvider:authProvider,contactPoints : cassandra_ob.credentials["node_ips"]} );
client.connect(function(err, result) {
    console.log('Connected.');
});

/*
var client = new cassandra.Client( {contactPoints: ['172.17.0.1']} );
client.connect(function(err, result) {
    console.log('Connected.');
});
*/
app.get('/metadata', function(req, res) {
    res.send(client.hosts.slice(0).map(function (node) {
        return { address : node.address, rack : node.rack, datacenter : node.datacenter }
    }));
});
app.all("/", function (req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
    res.header("Access-Control-Allow-Methods", "GET, PUT, POST");
    return next();
});

/**** Create Table for DB****/
 function create_tables()
 {
    console.log("Tables created");
    async.series([
        function(next) {
            client.execute('CREATE TABLE IF NOT EXISTS '+keyspace_name+'.account (BAN bigint,MSTSDN bigint,customer_type text,sub_status text,first_name text,last_name text,email text,PRIMARY KEY(BAN));',
                next);
        },
        function(next) {
            client.execute('CREATE TABLE IF NOT EXISTS '+keyspace_name+'.address(BAN bigint,address_id bigint,address_line1 text,city_name text,state_code text,postal_code text,PRIMARY KEY(BAN,address_id));',
                next);
        },
        function(next) {
            client.execute('CREATE TABLE IF NOT EXISTS '+keyspace_name+'.plan(BAN bigint,plan_id bigint,plan_type text,plan_info text,PRIMARY KEY(BAN,plan_id));',
                next);
        },
        function(next) {
            client.execute('CREATE TABLE IF NOT EXISTS '+keyspace_name+'.card_details(BAN bigint,card_id bigint,card_category text, card_type text, security_code text, expiration_date text,card_number text,PRIMARY KEY(BAN,card_id));',
                next);
        }    
        ],  function(err,result)
        {
            console.log(err);
            if(result!=null)
            console.log(result);
        });
}
/*app.post('/keyspace', function(req, res) {
    console.log("asdasasdadasd");
    client.execute("CREATE KEYSPACE IF NOT EXISTS default_info WITH replication " + 
                   "= {'class' : 'SimpleStrategy', 'replication_factor' : 3};",
                   afterExecution('Error: ', '` created.', res));
});*/

app.post('/delete', function(req, res) {
    async.series([
        function(next) {   
                client.execute('drop table '+keyspace_name+'.account;',next)

        }],   function(err,result)
        {
            console.log(err);
            if(result!=null)
            console.log(result);
        });
});




app.post('/account', function(req, res) {

 var insertAccountInfo='INSERT INTO '+keyspace_name+'.account (BAN,MSTSDN,customer_type,sub_status ,first_name ,last_name ,email ) values (?,?,?,?,?,?,?);';


    client.execute(insertAccountInfo,
        [Long.fromString(req.body.BAN),Long.fromString(req.body.MSTSDN),req.body.customer_type,req.body.sub_status,req.body.first_name, 
        req.body.last_name,req.body.email],

        function(err,result)
        {
            console.log(err);
            if(result!=null)
            console.log(result);
        });
   
    res.send("Account Number "+req.body.BAN+" Inserted Successfully");
});


app.get('/kill', function(req, res) {
    process.exit()
});

 app.get('/account/:id', function(req, res) {

  var  getAccountById='SELECT * FROM '+keyspace_name+'.account WHERE BAN = ?;';
    client.execute(getAccountById, [ Long.fromString(req.params.id) ], function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'Account not found.' });
        } else {
            //res.send(result)
            res.send(result.rows.map(function (node) {
       
        return { BAN:node.BAN,  MSTSDN: node.MSTSDN, customer_type: node.customer_type, sub_status: node.sub_status, first_name: node.first_name, 
         last_name: node.last_name, email : node.email


        }
    
    }));
              
             }
    });
});

 app.post('/account/change', function(req, res) {

var updateAccountInfo='update '+keyspace_name+'.account set first_name=?, last_name=?, email=? WHERE BAN=? ;';

    client.execute(updateAccountInfo,
        [req.body.first_name, req.body.last_name,req.body.email, Long.fromString(req.body.BAN)],
        function(err,result)
        {
            console.log(err);
            if(result!=null)
            console.log(result);
        });
      res.send("Update");

      
});

app.post('/address', function(req, res) {
  
 var insertAddressInfo='INSERT INTO '+keyspace_name+'.address (BAN ,address_id ,address_line1 ,city_name ,state_code ,postal_code ) values (?,?,?,?,?,?);';

    client.execute(insertAddressInfo,
        [Long.fromString(req.body.BAN),Long.fromString(req.body.address_id),req.body.address_line1,req.body.city_name,req.body.state_code, 
        req.body.postal_code],

        function(err,result)
        {
            console.log(err);
            if(result!=null)
            console.log(result);
        });

       res.setHeader('Content-Type', 'application/json');
    res.send("Address with BAN Number "+req.body.BAN+" Inserted Successfully")
});



 app.get('/address/:id', function(req, res) {

    var getAddressById='SELECT * FROM '+keyspace_name+'.address WHERE BAN = ?;';
    client.execute(getAddressById, [ Long.fromString(req.params.id) ], function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'Account not found.' });
        } else {
            //res.send(result)
            res.send(result.rows.map(function (node) {
       
        return { BAN:node.BAN,  ç: node.address_id, address_line1: node.address_line1, city_name: node.city_name, state_code: node.state_code, 
         postal_code: node.postal_code

        }
    
    }));
             
             }
    });
});

 app.get('/address/getalladdresses', function(req, res) {

    var getalladdresses='SELECT * FROM '+keyspace_name+'.address;';
    client.execute(getalladdresses, function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'Accounts not found.' });
        } else {
            //res.send(result)
            res.send(result.rows.map(function (node) {
       
        return { BAN:node.BAN,  address_id: node.address_id, address_line1: node.address_line1, city_name: node.city_name, state_code: node.state_code, 
         postal_code: node.postal_code

        }
    
    }));
             
             }
    });
});

 app.post('/address/change', function(req, res) {

var updateAddressInfo='update '+keyspace_name+'.address set address_line1=?, city_name=?, state_code=?, postal_code=? WHERE BAN=? and address_id=?;';

    client.execute(updateAddressInfo,
        [req.body.address_line1, req.body.city_name,req.body.state_code,req.body.postal_code, Long.fromString(req.body.BAN),Long.fromString(req.body.address_id)],
        function(err,result)
        {
            console.log(err);
            if(result!=null)
            console.log(result);
        });
       res.send("Update");
});

app.post('/plan', function(req, res) {
  
 
 var insertPlanInfo='INSERT INTO '+keyspace_name+'.plan (BAN,plan_id ,plan_type ,plan_info ) values (?,?,?,?);';


    client.execute(insertPlanInfo,
        [Long.fromString(req.body.BAN),Long.fromString(req.body.plan_id),req.body.plan_type,req.body.plan_info],

        function(err,result)
        {
            console.log(err);
            if(result!=null)
            console.log(result);
        });
          res.setHeader('Content-Type', 'application/json');
    res.send(" Plan Inserted for BAN Number "+req.body.BAN+"  Successfully")
});


 app.get('/plan/:id', function(req, res) {

    var getPlanById='SELECT * FROM '+keyspace_name+'.plan WHERE BAN = ?;';
    client.execute(getPlanById, [ Long.fromString(req.params.id) ], function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'Account not found.' });
        } else {
            //res.send(result)
            res.send(result.rows.map(function (node) {
       
        return { BAN:node.BAN,  plan_id: node.plan_id, plan_type: node.plan_type, plan_info: node.plan_info

        }
    
    }));
           
             }
    });
});

 app.post('/plan/change', function(req, res) {

var updatePlanInfo='update '+keyspace_name+'.plan set plan_type=?, plan_info=? WHERE BAN=? and plan_id=?;';

    client.execute(updatePlanInfo,
        [req.body.plan_type, req.body.plan_info, Long.fromString(req.body.BAN),Long.fromString(req.body.plan_id)],
        function(err,result)
        {
            console.log(err);
            if(result!=null)
            console.log(result);
        });
    res.send("Update");
      
});
app.post('/card_details', function(req, res) {
  
 
 var insertCardDetailsInfo='INSERT INTO '+keyspace_name+'.card_details (BAN ,card_id ,card_category , card_type , security_code , expiration_date ,card_number ) values (?,?,?,?,?,?,?);';


    client.execute(insertCardDetailsInfo,
        [Long.fromString(req.body.BAN),Long.fromString(req.body.card_id),req.body.card_category,req.body.card_type,req.body.security_code,req.body.expiration_date,req.body.card_number],

       
        function(err,result)
        {
            console.log(err);
            if(result!=null)
            console.log(result);
        });
        res.setHeader('Content-Type', 'application/json');
        res.send("Card Info with BAN "+req.body.BAN+" Inserted Successfully")
});


 app.get('/card_details/:id', function(req, res) {

    var getCardDetailsById='SELECT * FROM '+keyspace_name+'.card_details WHERE BAN = ?;';
    client.execute(getCardDetailsById, [ Long.fromString(req.params.id) ], function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'Account not found.' });
        } else {
            //res.send(result)
            res.send(result.rows.map(function (node) {
       
        return { BAN:node.BAN,  card_id: node.card_id, card_category: node.card_category, card_type: node.card_type, security_code: node.security_code, expiration_date: node.expiration_date, 
         card_number: node.card_number


        }
    
    }));
         
             }
    });
});

 app.post('/card_details/change', function(req, res) {

var updatePlanInfo='update '+keyspace_name+'.card_details set card_category=?, card_type=?, security_code=?, expiration_date=?, card_number=? WHERE BAN=? and card_id=?;';

    client.execute(updatePlanInfo,
        [req.body.card_category, req.body.card_type, req.body.security_code,req.body.expiration_date, req.body.card_number,Long.fromString(req.body.BAN),Long.fromString(req.body.card_id)],
        function(err,result)
        {
            console.log(err);
            if(result!=null)
            console.log(result);
        });
       res.send("Update");
});



app.post('/account_change', function(req, res) {

    client.execute(updateAccountInfo,
        [req.body.address_line1, req.body.city_name,req.body.state_code,req.body.postal_code,req.body.email, Long.fromString(req.body.account_number)],
        function(err,result)
        {
            console.log(err);
            if(result!=null)
            console.log(result);
        });
      
});


var server = app.listen( appEnv.port || 3000, function() {
    create_tables();
    console.log('Listening on port %d', server.address().port);
});
