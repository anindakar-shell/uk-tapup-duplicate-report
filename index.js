const express = require('express');
const dotenv = require('dotenv');
const { Pool, Client } = require('pg');
const _ = require('lodash');
const moment = require('moment');
const Decimal = require('decimal.js');
const converter = require('json-2-csv');
const fs = require('fs');

dotenv.config();
const app = express();

// const client = new Pool({
//   host: 'localhost',
//   port: 5432,
//   user: 'postgres',
//   password: 'tapup',
//   database: 'tapup3',
//   ssl: false,
//   max: 300
// });

const client = new Pool({
  host: 'ec2-52-50-55-241.eu-west-1.compute.amazonaws.com',
  port: 5432,
  user: 'u5i2d3bs6ncq3u',
  password: 'pd8e4c1851f3fe19bbbccb88547feedd3495049d5d5d79eca1ba5574a451a9308',
  database: 'dfeib2eljma0t3',
  ssl: true,
  max: 300
});

client.connect();

app.get('/', async (req, res) => {
  let startDate = '2021-06-01';
  const finalDate = '2021-12-31';
  const dayCharges = [];
  let finalCharges = [];

  console.log('Start date: ', startDate);
  console.log('Final date: ', finalDate);

  while (new Date(startDate) <= new Date(finalDate)) {
    const endDate = moment(moment(startDate, 'YYYY-MM-DD').add(1, 'd')).format('YYYY-MM-DD');

    console.log('----------------------------------------------------------------------------------------------------------'); 
    console.log('Start date: ', startDate);
    console.log('End date: ', endDate);
    console.log('----------------------------------------------------------------------------------------------------------');

    const { rows, rowCount } = await getAllVehicles({
      startDate,
      endDate
    });
    
    const overChargedData = await loopAllVehicles({
      startDate,
      endDate,
      vehicles: rows
    });

    dayCharges.push(overChargedData);
    finalCharges = [...finalCharges, ...overChargedData];
    startDate = moment(moment(startDate, 'YYYY-MM-DD').add(1, 'd')).format('YYYY-MM-DD')
  }

  converter.json2csv(JSON.parse(JSON.stringify(finalCharges)), (err, csv) => {
    if (err) {
      throw err;
    }

    // print CSV string
    console.log(csv);
    // write CSV to a file
    fs.writeFileSync('draft_charges.csv', csv);

    return res.json({
      charges: finalCharges
    });
  });
});

const getAllVehicles = async ({ startDate, endDate }) => {
  try {
    const query = `SELECT count(*), ol."serviceableObjectIdentifier" FROM
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
    ol."createdAt" >='${startDate} 02:30:00' and ol."createdAt" < '${endDate} 02:30:00'
	  group by ol."serviceableObjectIdentifier"
    ORDER BY ol."serviceableObjectIdentifier" asc;`;
    // console.log(`Query: ${query}`);

    return client.query(query);
  } catch (error) {
    console.error(error);  
  }
}

const loopAllVehicles = async ({
  startDate, 
  endDate,
  vehicles
}) => {
  const chargedData = [];
  console.log('Date range: ', startDate, ' - ', endDate);
  console.log('----------------------------------------------------------------------------------------------------------'); 
  for (const vehicle of vehicles) {
    console.log('Checking overcharges for vehicle: ', vehicle.serviceableObjectIdentifier);    
    const { rows, rowCount} = await getAllTransactions({
      startDate,
      endDate,
      vehicle
    });

    if (rows.length > 1) {
      const overChanges = await loopAllTransactions({
        startDate,
        endDate,
        transactions: rows
      });

      if (overChanges.length > 0) {
        for (const charges of overChanges) {
          for (const charge of charges) {
            chargedData.push(charge);
          }          
        }
      }
    }  
  }
  console.log('----------------------------------------------------------------------------------------------------------');
  return chargedData;
}

const getAllTransactions = async ({ startDate, endDate, vehicle }) => {
  try {
    const query = `SELECT ol."id", ol."OrderId", ol."ServiceableObjectId", ol."serviceableObjectIdentifier", ol."volumeFilled",ol."price", ol."createdAt", ol."updatedAt", ol."settlementFileId", sf."filename", ol."completedDate", c."name", cn."vat" FROM
    public."OrderLines" ol
    left join 
    public."Orders" o
    on o.id = ol."OrderId"
    left join 
    public."Customers" c 
    on c.id = o."CustomerId"
    left join 
    public."Countries" cn
    on cn.id = c."countryId"
    left join 
    public."SettlementFiles" sf 
    on sf.id = ol."settlementFileId"
    where 
    o."CustomerId" IN (
        SELECT id FROM public."Customers" where "shellCardNr" != ''
    )
    and
    (ol."createdAt" >='${startDate} 02:30:00' and ol."createdAt" < '${endDate} 02:30:00')
    and ol."serviceableObjectIdentifier" = '${vehicle.serviceableObjectIdentifier}'
    ORDER BY ol."settlementFileId" asc`;
    // console.log(`Query: ${query}`);

    return client.query(query);
  } catch (error) {
    console.error(error);  
  }
}

const loopAllTransactions = async ({
  startDate,
  endDate,
  transactions
}) => {
  const extraData = [];
  let overChargeDate = endDate;
  let totalVolumeCharged = 0;

  for (let i = 0; i < transactions.length; i++) {
    const overChrages = [];
    let sumVolume = 0;
    let unitPrice = 0;
    console.log('----------------------------------------------------------------------------------------------------------');

    // Overcharge date 
    overChargeDate = moment(moment(overChargeDate, 'YYYY-MM-DD').add(1, 'd')).format('YYYY-MM-DD')
    for (let j = i + 1; j <= transactions.length - 1; j++) {
      if (transactions[j - 1].settlementFileId != transactions[j].settlementFileId) {       
        console.log(overChargeDate, ' | Company: ', transactions[j].name, ' | Txn ID: ', transactions[j].id, ' | OrderId: ', transactions[j].OrderId, ' | License: ', transactions[j].serviceableObjectIdentifier, ' | Volume: ', transactions[j].volumeFilled, 'lt', ' | Unit Price: ', transactions[j].price, ' | File Id: ', transactions[j].settlementFileId, ' | File Name: ', transactions[j].filename);

        // add the volume
        sumVolume += transactions[j].volumeFilled;
        unitPrice = transactions[j].price;

        let fuelFilledLt = new Decimal(transactions[j].volumeFilled).dividedBy(1000);
        let unitPriceInPd = new Decimal(transactions[j].price).dividedBy(1000);
        let txnPrice = new Decimal(fuelFilledLt).times(unitPriceInPd);

        overChrages.push({
          companyName: transactions[j].name,
          overChargeDate,
          transactionId: transactions[j].id,
          orderId: transactions[j].OrderId,
          licensePlate: transactions[j].serviceableObjectIdentifier,
          fuelFilled: transactions[j].volumeFilled,
          fuelFilledInLt: fuelFilledLt,
          unitPrice: transactions[j].price,
          unitPriceInPd: unitPriceInPd,
          settlementFileId: transactions[j].settlementFileId,
          settleFilename: transactions[j].filename,
        });
      }
    }

    if (sumVolume > 0) {
      totalVolumeCharged += sumVolume;
      let fuelLt = new Decimal(sumVolume);
      fuelLt = fuelLt.dividedBy(1000);
      // console.log('...........................................................................................................');
      console.log('----------------------------------------------------------------------------------------------------------');
      console.log('Volume Charged: ', sumVolume, '(', fuelLt , ')', ' | Unit Price: ', unitPrice, '(', new Decimal(unitPrice).dividedBy(1000), ')');
    }
    extraData.push(overChrages);
  }

  // console.log('----------------------------------------------------------------------------------------------------------');
  console.log('Total Volume Charged: ', totalVolumeCharged, '(', new Decimal(totalVolumeCharged).dividedBy(1000) , ')');
  console.log('----------------------------------------------------------------------------------------------------------');

  return extraData;
}

// Not in use
const getDuplicateCharges = async (rows) => {
  const duplicateTransactions = [];
  // const { rows } = transactions;
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
      // console.log('sortedArray...');
      // console.log(sortedArray);

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

  return duplicateTransactions;
}

app.listen('7200', () => {
  console.log(`Express server listening on port 7200...`);
})
