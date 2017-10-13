import json
import requests

ES_ADDR = "http://es1.job-listing.dev.zippia.com:9200"

QUERY = {
  "size": 0,
  "aggs": {
    "sources": {
      "terms": {
        "field": "source.keyword",
        "size": 20
      },
      "aggs": {
        "prices": {
          "terms": {
            "field": "price",
            "size": 100
          }
        }
      }
    }
  }
}

rsp = requests.get(ES_ADDR + "/joblistings/normalizedJobs/_search", json=QUERY)
data = rsp.json()
simplified = {
    item["key"]: {
        "count": item["doc_count"],
        "prices": {
            price_item["key"]: price_item["doc_count"]
            for price_item in item["prices"]["buckets"]
        }
    }
    for item in data['aggregations']['sources']['buckets']
}

# print json.dumps(simplified, indent=2)


def print_csv(data):
    for feed, val in data.items():
        for price, count in val['prices'].items():
            print ",".join(map(str, [feed, price, count]))


def print_avg_price_per_feed(data):
    for feed, val in data.items():
        prices = val['prices']
        avg_price = 1.0 * sum([price * count for price, count in prices.items()]) / sum(prices.values())
        print ",".join(map(str, [feed, avg_price]))


def print_total_avg_price(data):
    total_price = 0
    total_count = 0
    for feed, val in data.items():
        for price, count in val['prices'].items():
            total_price += price * count
            total_count += count
    print(1.0 * total_price / total_count)


def print_avg_after_remove_highest_lowest(data):
    for feed, val in data.items():
        
        ten_percent_lowest = int(val['count'] * 0.1)
        ten_percent_highest = int(val['count'] * 0.1)
        # print val['prices']
        # print val['count']
        # print '10% :', feed, ten_percent_highest
        prices = sorted(val['prices'].items(), key=lambda item: item[0], reverse=True)
        # print val['prices']
        # print prices

        while ten_percent_highest > 0:
            ten_percent_highest = ten_percent_highest - prices[0][1]
            if ten_percent_highest >= 0:
                del prices[0]
            else:
                value = - ten_percent_highest
                key = prices[0][0]
                prices[0] = (key, value)

        while ten_percent_lowest > 0:
            ten_percent_lowest = ten_percent_lowest - prices[-1][1]
            if ten_percent_lowest >= 0:
                del prices[-1]
            else:
                value = - ten_percent_lowest
                key = prices[-1][0]
                prices[-1] = (key, value)

        # print prices
        print feed, prices
        prices2 = sorted(val['prices'].items(), key=lambda item: item[0], reverse=True)
        print feed, prices2
        prices = dict(prices)
        avg_price = 1.0 * sum([price * count for price, count in prices.items()]) / sum(prices.values())
        print ",".join(map(str, [feed, avg_price]))    


# print_csv(simplified)
# print_avg_price_per_feed(simplified)
# print_total_avg_price(simplified)
print_avg_after_remove_highest_lowest(simplified)