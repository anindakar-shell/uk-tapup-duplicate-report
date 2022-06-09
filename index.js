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
  host: process.env.host,
  port: process.env.port,
  user: process.env.user,
  password: process.env.password,
  database: process.env.database,
  ssl: true,
  max: 300
});

client.connect();

app.get('/', async (req, res) => {
  let startDate = '2022-01-01';
  const finalDate = '2022-05-31';
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

    // console.log('All transactions...');
    // console.log(rows);

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
    const query = `SELECT ol."id", ol."OrderId", ol."ServiceableObjectId", ol."serviceableObjectIdentifier", 
    ol."volumeFilled",ol."price", ol."createdAt", ol."updatedAt", ol."settlementFileId", sf."filename", ol."completedDate", 
    c."name", cn."vat", to_char(ol."createdAt", 'YYYY-MM-DD') as createDate, to_char(ol."createdAt", 'HH24:MI') as createTime,
    o."CustomerLocationId", o."CustomerId", cn."timezone", ol."productId", ol."productName"
    FROM
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
    let contractFee = 0;
    let contractDate = '';
    let contractVehicle = '';
    let contractCompany = '';

    console.log('----------------------------------------------------------------------------------------------------------');

    // Overcharge date 
    overChargeDate = moment(moment(overChargeDate, 'YYYY-MM-DD').add(1, 'd')).format('YYYY-MM-DD');
    for (let j = i + 1; j <= transactions.length - 1; j++) {
      if (transactions[j - 1].settlementFileId != transactions[j].settlementFileId) {
        console.log(overChargeDate, transactions[j].createdate, transactions[j].createtime, ' | Company: ', transactions[j].name, ' | Txn ID: ', transactions[j].id, ' | OrderId: ', transactions[j].OrderId, ' | License: ', transactions[j].serviceableObjectIdentifier, ' | Volume: ', transactions[j].volumeFilled, 'lt', ' | Unit Price: ', transactions[j].price, ' | File Id: ', transactions[j].settlementFileId, ' | File Name: ', transactions[j].filename);

        // Customer contract
        const { rows, rowCount } = await getCustomerContracts(transactions[j].CustomerId);
        // console.log('Customer contracts...');
        // console.log(rows);

        const customerContract = await filterCustomerContractWithRange(transactions[j], rows);
        const discountAmount = (customerContract && (customerContract.ProductId === transactions[j].productId)) ? customerContract.discountAmount : 0;
        contractFee = (customerContract && customerContract.serviceFee) ? new Decimal(customerContract.serviceFee).dividedBy(1000) : 0;
        contractDate = transactions[j].createdate;
        contractVehicle = transactions[j].serviceableObjectIdentifier;
        contractCompany = transactions[j].name;

        console.log('Customer discount...');
        console.log(customerContract);
        console.log(discountAmount);

        // add the volume
        sumVolume += transactions[j].volumeFilled;
        unitPrice = transactions[j].price;

        const countryVat = new Decimal(transactions[j].vat).add(1);
        const fuelFilledLt = new Decimal(transactions[j].volumeFilled).dividedBy(1000);
        const unitPriceInPd = new Decimal(transactions[j].price).dividedBy(1000);
        const priceAfterDiscount = new Decimal(unitPriceInPd).add(discountAmount);
        const unitPriceGross = new Decimal(priceAfterDiscount).times(countryVat);
        const totalPrice = new Decimal(priceAfterDiscount).times(fuelFilledLt);
        const totalPriceIncludingTax = new Decimal(totalPrice).times(countryVat);

        overChrages.push({
          companyName: transactions[j].name,
          transactionCreateDate: transactions[j].createdate,
          transactionCreateTime: transactions[j].createtime,
          transactionId: transactions[j].id,
          orderId: transactions[j].OrderId,
          licensePlate: transactions[j].serviceableObjectIdentifier,
          fuelFilled: transactions[j].volumeFilled,
          fuelFilledInLt: fuelFilledLt,
          unitPrice: transactions[j].price,
          unitPriceInPd: unitPriceInPd,
          discountAmount,
          countryVat,
          priceAfterDiscount,
          unitPriceGross,
          totalPrice,
          totalPriceIncludingTax,
          serviceFee: "",
          settlementFileId: transactions[j].settlementFileId,
          settleFilename: transactions[j].filename,
          overChargeDate,
        });
      }
    }

    if (sumVolume > 0) {
      // For the service fee
      overChrages.push({
        companyName: contractCompany,
        transactionCreateDate: contractDate,
        transactionCreateTime: "",
        transactionId: "",
        orderId: "",
        licensePlate: contractVehicle,
        fuelFilled: "",
        fuelFilledInLt: "",
        unitPrice: "",
        unitPriceInPd: "",
        discountAmount: "",
        countryVat: "",
        priceAfterDiscount: "",
        unitPriceGross: "",
        totalPrice: "",
        totalPriceIncludingTax: "",
        serviceFee: contractFee,
        settlementFileId: "",
        settleFilename: "",
        overChargeDate: "",
      });

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

const getCustomerContracts = async (customerId) => {
  try {
    const query = `select cont."id", cont."startDate", cont."endDate", cont."priceType", 
    ccpd."ProductId", ccpd."discountAmount", cont."serviceFee",
    extract(epoch from cont."startDate") as startDateUx,
    extract(epoch from cont."endDate") as endDateUx
    from "Contracts" cont
    left join
    "CustomerContractProductDiscounts" ccpd on ccpd."contractId" = cont.id
    WHERE cont."CustomerId" = ${customerId}`;
    // console.log(`Query: ${query}`);

    return client.query(query);
  } catch (error) {
    console.error(error);
  }
}

const filterCustomerContractWithRange = async (transaction, contracts) => {
  return contracts.find(c => (transaction.completedDate >= c.startdateux && transaction.completedDate <= c.enddateux));
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
