import numpy as np
import math
import boto3
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

MAX_WAIT_TIME = 10  # s

# house address
df_mls = pd.read_csv('/media/qindom-cpu/wd1/kai/real_master_crawler/house_sigma/mls_sample_with_url_to_20190430.csv',
                     index_col=[0])
urls = list(df_mls['house_sigma_url'])
mlss = list(df_mls['_id'])
n_mls = len(mlss)
print('num urls: %d' % n_mls)

chrome_options = webdriver.ChromeOptions()

chrome_options.add_argument('--headless')
driver = webdriver.Chrome('/var/qindom/chromedriver', chrome_options=chrome_options)

'''signin first'''
print('signin...')
root_url = 'https://housesigma.com'

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

'''step 2, get listing contents'''
# example: W4436597
batch_size = 1000
n_batch = int(n_mls / batch_size) + 1

start = time.time()
for i_batch in range(n_batch):
    print('batch %d/%d' % (i_batch, n_batch))

    contents = []
    for j in range(batch_size):
        ind = i_batch * batch_size + j

        if ind < n_mls:
            url, mls = urls[ind], mlss[ind]
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

                contents.append([mls, page_source,
                                 content_market, content_estimate_selling_price, content_estimate_renting_price])
                print('%s page source len: %d; market: %0.1f; pred selling: %d; pred renting: %d' %
                      (mls, len(page_source),
                       content_market, content_estimate_selling_price, content_estimate_renting_price))

    print('saving batch %d' % i_batch)
    df_contents = pd.DataFrame(contents, columns=['_id', 'page_source', 'market_bias', 'pred_sell', 'pred_rent'])
    df_contents.to_csv('/media/qindom-cpu/wd1/kai/real_master_crawler/house_sigma/content_1/house_sigma_content1_' + str(i_batch) + '.csv')

# # upload to s3
# s3 = boto3.Session(
#         aws_access_key_id='AKIA2OKWCC2CQRPZWOOJ',
#         aws_secret_access_key='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',
#     ).resource('s3')
#
# s3.Object('kai-data-source', 'house_sigma_content.csv').put(Body=open('house_sigma_content.csv', 'rb'))
# s3.ObjectAcl('kai-data-source', 'house_sigma_content.csv').put(ACL='public-read')

driver.quit()
