import re
import itertools
from multiprocessing.pool import ThreadPool

import requests
from bs4 import BeautifulSoup

from models.db_models import session, BuildingPermit


def generate_post_data(months_range, years_range):
    """
    months_range: tuple with integers where month format is %m
    years_range: tuple with integers where year format is %yyyy
    """
    months = ["0" * (2 - len(str(month))) + str(month) for month in range(months_range[0], months_range[1])]
    years = [str(year) for year in range(years_range[0], years_range[1])]
    states = ['01Alabama', '02Alaska', '04Arizona', '05Arkansas', '06California', '08Colorado', '09Connecticut',
              '10Delaware', '11DistrirctofColumbia', '12Florida', '13Georgia', '15Hawaii', '16Idaho', '17Illinois',
              '18Indiana', '19Iowa', '20Kansas', '21Kentucky', '22Louisiana', '23Maine', '24Maryland',
              '25Massachusetts', '26Michigan', '27Minnesota', '28Mississippi', '29Missouri', '30Montana', '31Nebraska',
              '32Nevada', '33NewHampshire', '34NewJersey', '35NewMexico', '36NewYork', '37NorthCarolina',
              '38NorthDakota', '39Ohio', '40Oklahoma', '41Oregon', '42Pennsylvania', '44RhodeIsland', '45SouthCarolina',
              '46SouthDakota', '47Tennessee', '48Texas', '49Utah', '50Vermont', '51Virginia', '53Washington',
              '54WestVirginia', '55Wisconsin', '56Wyoming']

    search_combos = itertools.product(months, years, states)

    post_data_list = []

    for search_combo in search_combos:
        post_data = {"checkCounty": "Place",
                     "S": search_combo[2],  # state, "01Alabama"
                     "r": "Place",
                     "I": "6",
                     "o": "Monthly",
                     "M": search_combo[0],  # month, "06"
                     "Y": search_combo[1]  # year, "2004"
                     }

        post_data_list.append(post_data)

    return post_data_list


def get_census_reponse(post_data):
    resp = requests.post("http://censtats.census.gov/cgi-bin/bldgprmt/bldgbrowse.pl",
                         data=post_data,
                         headers={"User-Agent": "Mozilla/5.0"})
    return post_data, resp


def parse_results(results_list):
    results_array = []
    for t in results_list:
        response = t[1]
        post_data = t[0]
        soup = BeautifulSoup(response.text)
        results_table = soup.find_all("table")[-2]
        city_rows = results_table.findAll("tr", attrs={"align": "right"})
        if city_rows:
            for row in city_rows:
                state_name = ''.join([char for char in post_data['S'] if not char.isdigit()])
                state_name = ''.join([" " + char if char.isupper() else char for char in state_name]).strip()
                city_name = row.find_all("td")[1].text
                city_name = re.sub(r'\(.*\)', '', city_name).strip()
                result_dict = {
                    "month": post_data['M'] + '-' + post_data['Y'],
                    "state": state_name,
                    "city": city_name,
                    "num_buildings": int(row.find_all("td")[2].text.replace(",", "")),
                    "num_units": int(row.find_all("td")[3].text.replace(",", "")),
                    "construction_cost": int(row.find_all("td")[4].text.replace(",", ""))
                }
                results_array.append(result_dict)

    return results_array


def main():
    post_data_list = generate_post_data((1, 12), (2005, 2014))

    pool = ThreadPool(5)
    results = pool.map(get_census_reponse, post_data_list[:10])
    results_array = parse_results(results)

    session.add_all([BuildingPermit(**row) for row in results_array])
    session.commit()


if __name__ == '__main__':
    main()
