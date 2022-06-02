const { Client } = require('pg');
const  _ = require('lodash');
const { sum, forEach } = require('lodash');

require('dotenv').config();

module.exports.getClient = async () => {
  const client = new Client({
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: 'tapup',
    database: 'tapup3',
    ssl: false
  });
 
  await client.connect();
  const query = `SELECT ol."OrderId", ol."ServiceableObjectId", ol."serviceableObjectIdentifier", ol."volumeFilled",ol."price", ol."createdAt", ol."settlementFileId",  ol."completedDate", c."name" FROM
  public."OrderLines" ol
  left join 
  public."Orders" o
  on o.id = ol."OrderId"
  left join 
  public."Customers" c 
  on c.id = o."CustomerId"
  where 
  o."CustomerId" IN (
      SELECT id FROM public."Customers" where "shellCardNr" != ''
  )
  and
  ol."createdAt" >='2022-05-01 02:30:00' and ol."createdAt" < '2022-05-07 02:30:00'
  ORDER BY ol."settlementFileId" asc`;

  const transactions = await client.query(query);
  const duplicateTransactions = [];
  const { rows } = transactions;
  // grouping companywise
  const companyWiseGrouped = rows.reduce( (newArray, value) => {
    if(!newArray[value.name]) {
      newArray[value.name] = [];
    }
    newArray[value.name].push(value);
    return newArray;
  }, {});
  // console.log('companyWiseGrouped', companyWiseGrouped)
  for(var key in companyWiseGrouped) {
    let comapnyFinding = {
      'companyName': key
    };
    const vehicleWiseGrouped = _.groupBy(companyWiseGrouped[key], (transaction) => transaction.serviceableObjectIdentifier);
    // console.log('vehicleWiseGrouped', vehicleWiseGrouped);
    const vehicles = [];
    for (var vehicleKey in vehicleWiseGrouped) {
      let vehicleFinding = {};
      vehicleFinding.licensePlate = vehicleKey;
      vehicleFinding.totalTransactions = vehicleWiseGrouped[vehicleKey].length;
      let charges = [];
      const currentArray = vehicleWiseGrouped[vehicleKey];
      const filteredArray = _.filter(currentArray, ({ settlementFileId }) => settlementFileId != null);
      const sortedArray = _.sortBy(filteredArray, (currentObject) => currentObject.settlementFileId);
      console.log('sortedArray...');
      console.log(sortedArray);

      let total_sum = _.sumBy(sortedArray, (object) => object.volumeFilled);
      // console.log('total_sum', total_sum)
      if (sortedArray.length > 1) {
        let curr_sum = 0;
        let tempArray = sortedArray;
        for (let i = 0; i < sortedArray.length - 1 ; i++) {
          let order = {};
          curr_sum = sortedArray[i].volumeFilled;
          total_sum -= curr_sum;
          if(!_.isEqual(sortedArray[i].settlementFileId, sortedArray[i+1].settlementFileId)) {
            let txns = [];
            // tempArray.shift();
            order.volumeFilled = total_sum;
            order.numberOfTransaction = sortedArray.length - i - 1;
            // order.transactions = [...sortedArray];
            charges.push(order);
          }
        }
        vehicleFinding.charges = charges;
        vehicles.push(vehicleFinding);
      }
      comapnyFinding.vehicles = vehicles;
    }
    duplicateTransactions.push(comapnyFinding);
  }
  // console.log('duplicateTransactions', duplicateTransactions)
  for (let i = 0; i < duplicateTransactions.length; i++) {
    const vehicleArray = duplicateTransactions[i].vehicles;
    console.log('company name', duplicateTransactions[i].companyName)
    for (let j = 0; j < vehicleArray.length; j ++ ) {
      console.log('duplicateTransactions', vehicleArray[j]);
      // console.log('Transactions...');
      // console.log(vehicleArray[j]['transactions']);
    }
  }
};

this.getClient();


// add customer name as well
