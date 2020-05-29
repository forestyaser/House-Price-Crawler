import numpy as np
import math
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

MAX_WAIT_TIME = 10  # s


def craw_mlss(df_mls):
    mlss = list(df_mls['_id'])
    n_mls = len(mlss)
    print('num urls: %d' % n_mls)

    chrome_options = webdriver.ChromeOptions()

    chrome_options.add_argument('--headless')
    driver_url = webdriver.Chrome('/var/qindom/chromedriver', chrome_options=chrome_options)  # for url
    driver = webdriver.Chrome('/var/qindom/chromedriver', chrome_options=chrome_options)  # for content

    '''signin first'''
    print('signin...')
    root_url = 'https://housesigma.com'

    driver_url.get(url=root_url + '/web/en')

    driver.get(url=root_url + '/web/en/login')
    WebDriverWait(driver, MAX_WAIT_TIME).until(
                EC.visibility_of_element_located((By.XPATH, '//*[@id="pane-email"]/form/div[1]/div/div/input'))
            )
    inputElement = driver.find_element(By.XPATH, '//*[@id="pane-email"]/form/div[1]/div/div/input')
    inputElement.send_keys('nuhubam@skymailapp.com')

    time.sleep(1)

    inputElement = driver.find_element(By.XPATH, '//*[@id="pane-email"]/form/div[2]/div/div/input')
    inputElement.send_keys('123456')

    time.sleep(1)

    login_button = driver.find_element(By.XPATH, '//*[@id="login"]/div[1]/div/div[3]/button')
    login_button.click()

    WebDriverWait(driver, MAX_WAIT_TIME).until(
                EC.visibility_of_element_located((By.XPATH, '//*[@id="index"]/div[1]/div[2]/div[1]/div[2]'))
            )

    time.sleep(1)

    # example: W4436597
    start = time.time()
    contents = []
    for ind in range(n_mls):
        mls = mlss[ind]
        '''step 1, get mls url'''
        inputElement_url_search = driver_url.find_element_by_id('search_input')

        inputElement_url_search.clear()
        inputElement_url_search.send_keys(mls)

        mls_href = ''
        try:
            element = WebDriverWait(driver_url, MAX_WAIT_TIME).until(
                EC.visibility_of_element_located(
                    (By.XPATH, '//*[@id="index"]/div[1]/div[2]/div[1]/div[2]/div[2]/div/p[2]'))
            )

            page_source = driver_url.page_source
            mls_ind = page_source.find(mls)
            from_ind = page_source.rfind('href="', 0, mls_ind) + len('href="')
            to_ind = mls_ind + len(mls)

            mls_href = page_source[from_ind + len('/web/en/house/'):to_ind]
        except:
            print('%s href not found. is the max waiting time too short?' % mls)

        '''step 2, get listing contents'''
        url = mls_href
        time_average = (time.time() - start) / (ind + 0.01)
        print('processing %d/%d: %s. (avg time: %0.1f s, est time: %d s)' %
              (ind, n_mls, mls, time_average, int(time_average * n_mls)))

        content_market, content_estimate_selling_price, content_estimate_renting_price = -1, -1, -1
        page_source = ''

        if (not isinstance(url, str)) or (len(url) < 6):
            print('%s not found in housesigma' % mls)
        else:
            full_url = root_url + '/web/en/house/' + url
            print(full_url)
            driver.get(url=full_url)

            try:
                WebDriverWait(driver, MAX_WAIT_TIME).until(
                    EC.visibility_of_element_located((
                        By.XPATH, '//*[@id="room_count_to_estimate"]/div[6]/div[1]/div[1]/div[2]/span'))
                )
                page_source = driver.page_source
            except:
                print('%s page not loaded.' % mls)

            try:
                # buyer/seller market
                anckor = 'margin-left: calc('
                from_ind = page_source.find(anckor)
                to_ind = page_source.find('%', from_ind)

                content_market = float(page_source[from_ind + len(anckor):to_ind])
            except:
                pass
                # print('%s market not found.' % mls)

            try:
                # estimate selling price
                from_ind = page_source.find('>SigmaEstimate<')
                lower_bound = page_source.find('>Rental')
                from_ind = page_source.find('>$', from_ind, lower_bound) + len('>$')
                to_ind = page_source.find('<', from_ind)

                content_estimate_selling_price = int(page_source[from_ind:to_ind].replace(',', ''))
            except:
                pass
                # print('%s predict selling price not found.' % mls)

            try:
                # estimate renting price
                from_ind = page_source.find('>Rental Estimate<')
                lower_bound = page_source.find('>School')
                from_ind = page_source.find('>$', from_ind, lower_bound) + len('>$')
                to_ind = page_source.find('<', from_ind)

                content_estimate_renting_price = int(page_source[from_ind:to_ind].replace(',', ''))
            except:
                pass
                # print('%s predict renting price not found.' % mls)

            contents.append([mls, url, page_source,
                             content_market, content_estimate_selling_price, content_estimate_renting_price])
            print('%s page source len: %d; market: %0.1f; pred selling: %d; pred renting: %d' %
                  (mls, len(page_source),
                   content_market, content_estimate_selling_price, content_estimate_renting_price))

    driver_url.quit()
    driver.quit()

    print('crawling finished.')
    df_contents = pd.DataFrame(contents, columns=['_id', 'url', 'page_source', 'market_bias', 'pred_sell', 'pred_rent'])
    return df_contents


if __name__ == '__main__':
    # house address
    # /media/qindom-cpu/wd1/kai/real_master_crawler/house_sigma/
    df_mls = pd.read_csv('mls_sample_to_20190430.csv',
                         index_col=[0])

    df_res = craw_mlss(df_mls.iloc[:5, :])
    print(df_res[['_id', 'url', 'market_bias', 'pred_sell', 'pred_rent']].to_string())


