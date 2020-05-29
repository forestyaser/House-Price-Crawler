import boto3
from time import time
import sys
# sys.stdout = open('log.txt', 'w')

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# house address
df_mls = pd.read_csv('/media/qindom-cpu/wd1/kai/real_master_crawler/house_sigma/mls_sample_to_20190430.csv', index_col=[0])
mlss = list(df_mls['_id'])
n_mls = len(mlss)
print('mlss total %d. est time: %0.1f h' % (n_mls, n_mls * 2 / 3600.))
start = time()

root_url = 'https://housesigma.com'
chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome('/var/qindom/chromedriver', chrome_options=chrome_options)

'''step 1, get mls url'''
driver.get(url=root_url + '/web/en')
driver.implicitly_wait(4)

inputElement = driver.find_element_by_id('search_input')

hrefs = []
for i, mls in enumerate(mlss):
    print('processing %s, %d/%d:' % (mls, i + 1, n_mls))

    inputElement.clear()
    inputElement.send_keys(mls)

    mls_href = ''
    try:
        element = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="index"]/div[1]/div[2]/div[1]/div[2]/div[2]/div/p[2]'))
        )

        page_source = driver.page_source
        mls_ind = page_source.find(mls)
        from_ind = page_source.rfind('href="', 0, mls_ind) + len('href="')
        to_ind = mls_ind + len(mls)

        mls_href = page_source[from_ind + len('/web/en/house/'):to_ind]
    except:
        print('%s href not found. is the max waiting time too short?' % mls)

    hrefs.append(mls_href)
    print(mls_href)

df_mls['house_sigma_url'] = hrefs
file_save = 'mls_sample_with_url_to_20190430.csv'
df_mls.to_csv(file_save)

# upload to s3
# s3 = boto3.Session(
#         aws_access_key_id='AKIA2OKWCC2CQRPZWOOJ',
#         aws_secret_access_key='R74CNLr5qZN+9f7TWBKEuDmV4RuzjRWQ6CG/+acN',
#     ).resource('s3')
#
# s3.Object('kai-data-source', file_save).put(Body=open(file_save, 'rb'))
# s3.ObjectAcl('kai-data-source', file_save).put(ACL='public-read')

driver.quit()

print('time cost: %d' % (int(time() - start)))
