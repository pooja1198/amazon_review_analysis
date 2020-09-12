from scrapy.selector import Selector
import requests
import re
import pandas as pd
from pathlib import Path

link_header = "https://www.amazon.in/Yogabar-Wholegrain-Breakfast-Muesli-Fruits/product-reviews/B07M6KZQCN/ref=cm_cr_getr_d_paging_btm_next_16?pageNumber="


def get_page_data(page_number):
    """
    This definition is used to extract the desired raw data from a single page of reviews from Amazon.
    We use the Scrapy Selector to get the required data by searching with the help of relative xpath.
    input: page_number(int)
    output: required data in the form of a list
    """

    url = link_header + str(page_number)
    r = requests.get(url)
    page_source = r.text
    sel_page = Selector(text=page_source)
    names = sel_page.xpath(
        '//*[@data-hook="review"]/div/div/div[1]/a/div[2]/span/text()'
    ).getall()
    dates_str = sel_page.xpath('//*[@data-hook="review"]/div/div/span/text()').getall()
    indices = list(map(lambda x: re.search(r"\d", x).start(), dates_str))
    for i in range(len(dates_str)):
        dates_str[i] = dates_str[i][indices[i]:]
    month = list(map(lambda x: str.split(x)[1], dates_str))
    year = list(map(lambda x: int(str.split(x)[2]), dates_str))
    stars = sel_page.xpath(
        '//*[@data-hook="review"]/div/div/div[2]/a[1]/i/span/text()'
    ).getall()
    size = sel_page.xpath('//*[@data-hook="review"]/div/div/div[3]/a/text()').getall()
    size = list(map(lambda x: x[x.find(" "):], size))
    rating = list(map(lambda x: int(str.split(x, ".")[0]), stars))
    review_heading = sel_page.xpath(
        '//*[@data-hook="review"]/div/div/div[2]/a[2]/span/text()'
    ).getall()
    review_heading = list(map(lambda x: x.lower(), review_heading))
    review_heading = list(map(lambda x : x[:-1] if x[-1] == '.' else x, review_heading))
    return [names, rating, dates_str, month, year, review_heading, size]


def implement_pagination():
    """
    In this definition we interate over 80 pages of reviews on Amazon and call the get_page_data definition to get the required data from each page.
    Finally, all the accumulated data from various pages is structured together to form a pandas Data Frame and is returned as output.
    """

    page_number = 0
    names_arr = list()
    ratings_arr = list()
    dates_arr = list()
    months_arr = list()
    years_arr = list()
    size_arr = list()
    headings_arr = list()
    while page_number < 86:
        try:
            page_number = page_number + 1
            page_data = get_page_data(page_number)
            names_arr = names_arr + page_data[0]
            ratings_arr = ratings_arr + page_data[1]
            dates_arr = dates_arr + page_data[2]
            months_arr = months_arr + page_data[3]
            years_arr = years_arr + page_data[4]
            headings_arr = headings_arr + page_data[5]
            size_arr = size_arr + page_data[6]
        except Exception as e:
            print("Exception was : ", e)
            print("The last page was : ", page_number - 1)
            break
    dict = {
        "Name": names_arr,
        "Rating (out of 5)": ratings_arr,
        "Size of Product": size_arr,
        "Date of Posting Review": dates_arr,
        "Month": months_arr,
        "Year": years_arr,
        "Review Title": headings_arr,
    }
    review_df = pd.DataFrame(dict)
    return review_df


if __name__ == "__main__":
    review_df = implement_pagination()
    project_dir = str(Path(__file__).resolve().parents[2])
    parent_folder = project_dir + "/data/raw/"
    review_df["Response"] = ""
    review_df.loc[review_df["Rating (out of 5)"] < 3, "Response"] = "Negative"
    review_df.loc[review_df["Rating (out of 5)"] == 3, "Response"] = "Neutral"
    review_df.loc[review_df["Rating (out of 5)"] > 3, "Response"] = "Positive"
    str_positive = " ".join(
        review_df.loc[review_df["Response"] == "Positive", "Review Title"]
    )
    str_negative = " ".join(
        review_df.loc[review_df["Response"] == "Negative", "Review Title"]
    )
    str_neutral = " ".join(
        review_df.loc[review_df["Response"] == "Neutral", "Review Title"]
    )
    positive_words = str.split(str_positive)
    negative_words = str.split(str_negative)
    neutral_words = str.split(str_neutral)
    words_df = pd.DataFrame(
        {
            "Positive Words": pd.Series(positive_words),
            "Negative Words": pd.Series(negative_words),
            "Neutral Words": pd.Series(neutral_words),
        }
    )
    words_df.to_csv(parent_folder + "Amazon_Review_Title_words.csv")
    review_df.to_csv(parent_folder + "Amazon_Reviews_Yogabar.csv")
