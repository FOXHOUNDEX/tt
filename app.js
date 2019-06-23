const _ = require('lodash')
const url = require('url')
const moment = require('moment')
const superagent = require('superagent')

const GET_TRANSACTIONS_URL = 'https://7np770qqk5.execute-api.eu-west-1.amazonaws.com/prod/get-transaction'
const POST_TRANSACTIONS_URL = 'https://7np770qqk5.execute-api.eu-west-1.amazonaws.com/prod/process-transactions'
const GET_EXCHANGE_RATES_URL = 'https://api.exchangeratesapi.io'

const TRANSACTIONS_TO_TAKE = 100

async function main() {
  try {
    const originalTransactions = await getTransactions(TRANSACTIONS_TO_TAKE)
    const transactions = addFormattedDates(originalTransactions)
    const datesAndCurrencies = extractDateAndCurrencies(transactions)

    const rates = await getExchangeRatesForDates(datesAndCurrencies)
    const convertedTransactions = convertTransactionRates(transactions, rates)
    const payload = { transactions: convertedTransactions }
    const result = await uploadTransactions(payload)

    console.log(result)
  } catch (err) {
    console.log(`ERROR: ${err.message}`)
  }
}

async function getExchangeRatesForDates(datesAndSymbols) {
  const dates = Object.keys(datesAndSymbols)
  const promises = dates.map(date => {
    const currencies = datesAndSymbols[date]
    return getSpecificRate(date, currencies).then(
      // date in response can be earlier, for example rate can be set in 2018-01-01 
      // and not changed within a month and we need a rate for 2018-01-07;
      // setting needed date here will help us to determine what rate we need to use for conversion
      result => {
        result.date = date
        return result
      }
    )
  })
  return Promise.all(promises)
}

async function getSpecificRate(date, currencies) {
  const finalUrl = url.resolve(GET_EXCHANGE_RATES_URL, date)
  const symbols = currencies.join(',')
  return superagent.get(finalUrl).query({ symbols }).then(r => r.body)
}

async function getTransactions(howMany) {
  return Promise.all(_.times(howMany, () => superagent.get(GET_TRANSACTIONS_URL).then(r => r.body)))
}

async function uploadTransactions(payload) {
  return superagent
    .post(POST_TRANSACTIONS_URL)
    .send(payload)
    .then(r => r.body)
}

// as a small optimization, format dates only once 
function addFormattedDates(transactions) {
  return transactions.map(tr =>
    Object.assign({}, tr, {
      createdAtFormatted: moment(tr.createdAt).format('YYYY-MM-DD')
    })
  )
}

function extractDateAndCurrencies(transactions) {
  const result = {}

  for (const tr of transactions) {
    const date = tr.createdAtFormatted
    if (!result[date]) {
      result[date] = []
    } else if (result[date].find(d => d === date)) {
      continue
    }

    result[date].push(tr.currency)
  }

  return result
}

function convertTransactionRates(transactions, actualRates) {
  return transactions.map(t => {
    const specificRate = actualRates.find(r => r.date === t.createdAtFormatted)
    const exchangeRate = specificRate.rates[t.currency]
    if (!exchangeRate) {
      throw new Error(`No rate for ${t.currency} currency`)
    }

    const newAmount = t.amount / exchangeRate
    if (isNaN(parseFloat(newAmount))) {
      throw new Error(`Result is not float`)
    }

    const convertedAmount = parseFloat(newAmount.toFixed(4)) // parsing again since toFixed returns string
    return _.omit(Object.assign({}, t, { convertedAmount }), 'exchangeUrl') // can use '...' here, but used the old one just in case
  })
}

main()