import luigi, csv

customerCountries = {}
stockPrices = {}
customerPurchaseLog = {}
customerPurchaseInvoices = {}
CUSTOMER_INFO = "customer_info.csv"
PRODUCT_INFO = "product_info.csv"
INVOICE = "invoice.csv"
anomalies = []

def parseCustomers(file):
    with open(file, 'r') as csvfile:
        has_header = csv.Sniffer().has_header(csvfile.read(1024))
        reader = csv.reader(csvfile, delimiter=',')
        if has_header:
            next(reader)
        for row in reader:
            customerId= row[0]
            country = row[1]
            customerCountries[customerId] = country

def parseStocks(file):
    with open(file, 'r') as csvfile:
        has_header = csv.Sniffer().has_header(csvfile.read(1024))
        reader = csv.reader(csvfile, delimiter=',')
        if has_header:
            next(reader)
        for row in reader:
            stockCode = row[0]
            price = float(row[2])
            stockPrices[stockCode] = price

def parseTransactions(file):
    with open(file, 'r') as csvfile:
        has_header = csv.Sniffer().has_header(csvfile.read(1024))
        reader = csv.reader(csvfile, delimiter=',')
        if has_header:
            next(reader)
        for row in reader:
            stockCode = row[1]
            quantity = row[2]
            customerId = row[4]
            if customerId not in customerCountries:
                'this is an invalid customerId, store it'
                anomalies.append(customerId + " is an invalid customerId for row " + ''.join(row))
            if stockCode in stockPrices:
                if customerId not in customerPurchaseInvoices:
                    amount = 0.0
                    amount += int(quantity) * stockPrices[stockCode]
                    customerPurchaseInvoices[customerId] = amount
                else:
                    amount = 0.0
                    amount = customerPurchaseInvoices[customerId]
                    amount += int(quantity) * stockPrices[stockCode]
                    customerPurchaseInvoices[customerId] = amount

                if customerId not in customerPurchaseLog:
                    bought = quantity + " of " + stockCode
                    customerPurchaseLog[customerId] = bought
                else:
                    bought = customerPurchaseLog[customerId]
                    bought += " " + quantity + " of " + stockCode
                    customerPurchaseLog[customerId] = bought
            else:
                'this is an invalid stock, store it'
                anomalies.append(stockCode + " is an invalid stockCode for row " + ''.join(row))

class ParseCustomerInfo(luigi.Task):

   def output(self):
       return luigi.LocalTarget("customerInfo.txt")

   def run(self):
       parseCustomers(CUSTOMER_INFO)
       with self.output().open("w") as out_file:
           for customer in customerCountries:
               out_file.write(customer + " : " + customerCountries[customer] + "\n")

class ParseProductDetails(luigi.Task):

    def output(self):
        return luigi.LocalTarget("stockInfo.txt")

    def run(self):
        parseStocks(PRODUCT_INFO)
        with self.output().open("w") as out_file:
            for stockCode in stockPrices:
                out_file.write(stockCode + " : " + str(stockPrices[stockCode]) + "\n")

class ParseCustomerTransactions(luigi.Task):

    def requires(self):
        return [ParseCustomerInfo(), ParseProductDetails()]

    def output(self):
        return luigi.LocalTarget("customerTransactions.txt")

    def run(self):
        parseCustomers(CUSTOMER_INFO)
        parseStocks(PRODUCT_INFO)
        parseTransactions(INVOICE)
        with self.output().open("w") as out_file:
            for customer in customerPurchaseLog:
                out_file.write(customer + " : " + " : " + str(customerPurchaseInvoices[customer]) + " : " + str(customerPurchaseLog[customer]) + "\n")
        with open("anomalies.txt","w") as file:
            for anomaly in anomalies:
                file.write(anomaly + "\n")

if __name__ == '__main__':
    luigi.run()