require('dotenv').config();
var express = require('express');
var express_graphql = require('express-graphql');
const { graphqlHTTP } = require('express-graphql');
var { buildSchema } = require('graphql');

// MySQL operational DB connection BEGIN
//env set in .env file
var mysql = require('mysql');
const con_mysql = mysql.createConnection({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USERNAME,
  password: process.env.MYSQL_PASS,
  database: process.env.MYSQL_DB_NAME
});
con_mysql.connect(function(error){
    if (!!error) {
        console.log("\n" + "-- > NO!, connection to the MySQL operational DB "  + process.env.MYSQL_DB_NAME + " on HOST "+process.env.MYSQL_HOST + " has failed!" + "\n");
    } else {
        console.log("\n" + "-- > YES, connection to the MySQL operational DB " + process.env.MYSQL_DB_NAME + " on HOST "+process.env.MYSQL_HOST +" has been successfully established !." + "\n");
    }
});
// MySQL operational DB connection END

// PostgreSQL decision DB connection BEGIN
const { Client }  = require('pg');
const con_pg = new Client({
    user: process.env.POSTGRESQL_USERNAME,
    host: process.env.POSTGRESQL_HOST,
    database: process.env.POSTGRESQL_DB_NAME,
    password: process.env.POSTGRESQL_PASS,
    port: process.env.POSTGRESQL_PORT
});
con_pg.connect(function(error){
    if (!!error) {
        console.log("\n" + "-- > NO!, connection to the PostgreSQL decision DB "  + process.env.POSTGRESQL_DB_NAME + " on HOST "+process.env.POSTGRESQL_HOST + " failed!" + "\n");
    } else {
        console.log("\n" + "-- > YES, connection to the PostgreSQL decision DB " + process.env.POSTGRESQL_DB_NAME + " on HOST "+process.env.POSTGRESQL_HOST +" has been successfully established!." + "\n");
    }
});
// PostgreSQL decision DB connection END


// GraphQL schema
// ! means that the field is non-nullable, meaning that the GraphQL service promises to always give you a value
// when you query this field. In the type language, we'll represent those with an exclamation mark.
var schema = buildSchema(`
scalar DateTime
    type Query {
        buildings(id: Int!): Building
        interventions(id: Int!): Intervention
        employees(id: Int!): Employee
    },

    type Building {
        id: Int!
        customer_id: Int
        admin_full_name: String
        admin_email: String
        admin_phone: String
        tech_contact_full_name: String
        tech_contact_email: String
        tech_contact_phone: String
        building_details: [Building_detail]
        interventions: [Intervention]
        address: Address
        customer: Customer
    }

    type Building_detail {
        id: Int!
        building_id: Int
        key: String
        value: String
    }

    type Address {
        id: Int!
        type_of_address: String
        status: String
        entity: String
        number_and_street: String
        appartment_or_suite: String
        city: String
        state: String
        zip_code: String
        country: String
        note: String
        longitude: Float
        latitude: Float
    }

    type Customer {
        id: Int!
        company_name: String
        full_name: String
        email: String
        address_id: Int
        user_id: Int
        phone: String
        company_description: String
        tech_authority_full_name: String
        tech_authority_email: String
        tech_authority_phone: String
    }

    type Employee {
        id: Int!
        user_id: Int
        email: String
        first_name: String
        last_name: String
        title: String
        building_details: [Building_detail]
        interventions: [Intervention]
        building: [Building]
    }

    type Intervention {
        id: Int!
        employee_id: Int
        building_id: Int
        battery_id: String
        column_id: String
        elevator_id: String
        building_details: [Building_detail]
        start_date: DateTime
        end_date: DateTime
        result: String
        report: String
        status: String
        address: Address
    }
`);

// Root resolver
var root = {
    buildings: getBuildings,
    interventions: getInterventions,
    employees: getEmployees,
    email: getEmail
};





//To answer Question 1 by intervention id
async function getInterventions({id}) {

    // Query the factintervention table from the PostgreSQL
    intervention = await query_postgresql('SELECT * FROM factintervention WHERE id = ' + id)
    console.log(intervention)
    resolve = intervention[0]
    
    // Query the MySQL address table.
    address = await query_mysql('SELECT * FROM addresses JOIN buildings ON buildings.address_id = addresses.id WHERE buildings.id = ' + resolve.building_id);
    console.log(address)
    resolve['address']= address[0];

    return resolve
};

async function getEmail({email}) {

   var email = await query_mysql('SELECT * FROM users WHERE email = ' + email)
   resolve = email[0]
   console.log(email)

   return resolve

};

//To answer Question 2 by building id
async function getBuildings({id}) {
    // Query building from the MySQL table
    var buildings = await query_mysql('SELECT * FROM buildings WHERE id = ' + id )
    resolve = buildings[0]
    console.log(buildings)

    // Query intervention from the PostgreSQL table
    interventions = await query_postgresql('SELECT * FROM factintervention WHERE building_id = ' + id)
    console.log(interventions)

    //Query customer info from the MySQL table
    customer = await query_mysql('SELECT * FROM customers WHERE id = ' + resolve.customer_id)

    resolve['customer']= customer[0];
    resolve['interventions']= interventions;

    return resolve
};

//To answer Question 3 by employee id
async function getEmployees({id}) {
    // Query employee from the MySQL table
    var employees = await query_mysql('SELECT * FROM employees WHERE id = ' + id )
    resolve = employees[0]
    console.log(employees)

    // Query intervention from the PostgreSQL table
    interventions = await query_postgresql('SELECT * FROM factintervention WHERE employee_id = ' + id)
    result = interventions[0]
    console.log(interventions)

    // Query building_details table in the MySQL DB to get key, value for each intervetion.
    var j;
    for (j = 0; j < interventions.length; j++){
    building_details = await query_mysql('SELECT * FROM building_details WHERE building_id = ' + interventions[j].building_id)
    interventions[j]['building_details']= building_details;
    console.log(building_details)
    }   
    resolve['interventions']= interventions;

    return resolve
};


// Function used to query the MySQL operational DB BEGIN
function query_mysql (queryStr) {
    console.log("-- > Run MySQL query : " + queryStr)
    return new Promise((resolve, reject) => {
        con_mysql.query(queryStr, function(err, result) {
            if (err) {
                return reject(err);
            } 
            return resolve(result)
        })
    })
};
// Function used to query the MySQL DB END


// Function used to query the PostgreSQL decision DB BEGIN
function query_postgresql(queryStr) {
    console.log("-- > Run PostgreSQL query : " + queryStr)
    return new Promise((resolve, reject) => {
        con_pg.query(queryStr, function(err, result) {
            if (err) {
                return reject(err);
            }
            return resolve(result.rows)
        })
    })
};
// Function used to query the PostgreSQL DB END


// Create an express server and a GraphQL endpoint
var app = express();
app.use('/graphql', graphqlHTTP({
    schema: schema,
    rootValue: root,
// Before prof, set to false to desactivate GraphiQL GUI on the route /graphql
    graphiql: true
}));
//PORT as env variable so Heroku can set a port
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => console.log(`-- > Express server started on port ${PORT}`));