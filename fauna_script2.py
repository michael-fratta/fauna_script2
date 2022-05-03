# python scheduler
import schedule
import time

def job(): # define the whole script as a function

    from dotenv import load_dotenv
    import os
    load_dotenv()

    import pandas as pd
    from datetime import datetime

    ## Google Drive API pre-amble
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    import json

    scopes = ['https://www.googleapis.com/auth/drive']

    secret_file = json.loads(os.getenv('google_secret'))

    creds = service_account.Credentials.from_service_account_info(secret_file, scopes=scopes)

    service = build('drive', 'v3', credentials=creds)

    # Call the Drive v3 API
    ## get all items in relevant folder
    results = service.files().list(
    pageSize=10, fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])

    # get today's date and reformat as string
    today = datetime.today().strftime('%Y%m%d')

    # create variable containing name of ratebook
    current_ratebook = f'ratebook_{today}.csv'

    # initiate file_found bool
    file_found = False

    # initialise slackbot func
    slack_token = os.getenv('slack_password')
    slack_channel = '#script-alerts'
    # create func
    def post_message_to_slack(text):
        return requests.post('https://slack.com/api/chat.postMessage', {
            'token': slack_token,
            'channel': slack_channel,
            'text': text,
        }).json()

    # find the relevant file_id, if there is a new ratebook
    for item in items:
        if current_ratebook in item['name']:
            file_id = item['id']
            file_found = True

    if file_found:

        ## get the new ratebook
        import requests
        access_token = creds.token
        url = "https://www.googleapis.com/drive/v3/files/" + file_id + "?alt=media"
        res = requests.get(url, headers={"Authorization": "Bearer " + access_token})

        ## print if file was found
        if res.ok:
            print(current_ratebook,"was found!")

        ## read the new ratebook as pandas dataframe
        import pandas as pd
        import io
        df = pd.read_csv(io.StringIO(res.text))

        # drop rows where 'AdjustedTotalRental' is 0
        df.drop(df[df['AdjustedTotalRental'] == 0].index, inplace=True)

        ## sort df by cap code, then by term, then by mileage
        df.sort_values(by=['CAPVersionCode','term','annualmileage'],inplace=True)

        ## reset index of sorted df (otherwise when I print everything it gies by i and will not print in order)
        df.reset_index(drop=True,inplace=True)


        ### transform ratebook from csv into cars_list

        # initialise cars list
        cars_list = []

        # initialise list idx
        idx = -1

        # first, cycle through all rows in ed rb
        for i in range(len(df)):
            
            # make it faster to search
            loc = df.loc[i]
            
            # then, assign relevant vars with their respective vals ## just need cap_code
            cap_code = loc['CAPVersionCode'].replace(' ', '')
            
            # from prices array:
            term_months = int(loc['term'])
            quote_mileage = int(loc['annualmileage'])
            price_pence = round(loc['AdjustedTotalRental']*100)
            finance_rental_pence = round(loc['AdjustedTotalRental']*100)
            service_rental_pence = round(loc['ServicesAmount']*100)
            p11d_pence = round(loc['TaxableListPrice']*100)
            excess_ppm = float(loc['MileageVariationAdjustment'])
            
            lender_name = "car_lender"
            
            # initialise cars dict
            cars = {}
            
            # assign car_specs dicts (only cap code needed for comparison)
            car_specs = {"cap_code": cap_code,
                        "prices":{lender_name:[]}}
            
            # assign car_prices dicts
            car_prices = [{"term_months": term_months,
                        "quote_mileage": quote_mileage,
                        "price_pence": price_pence,
                        "finance_rental_pence": finance_rental_pence,
                        "service_rental_pence": service_rental_pence,
                        "p11d_pence": p11d_pence,
                        "excess_ppm": excess_ppm},]
            
            # initialise exists to false
            exists = 0
            # see if capcode already in cars_list
            if len(cars_list) > 0:
                if cap_code in cars_list[idx]['cap_code']:
                    exists = 1
                else: exists = 0
            
            if exists == 0: # if cap_code not in cars_list
                if cars_list != None: # if cars_list is not empty
                    cars_list.append(cars) # append cars obj to cars_list li
                    cars.update(car_specs) # update cars obj with car_specs dict
                    cars['prices'][lender_name]+=car_prices #update cars obj with car_prices dict
                    idx+=1
                else: # if cars_list is empty
                # update the car obj
                    cars.update(car_specs)
                    cars['prices'][lender_name]+=car_prices
                    idx+=1
                
            else: # if cap_code already in cars_list
                cars_list[idx]['prices'][lender_name]+=car_prices # just append relevant car_prices to cars obj

        # connect to FaunaDB
        from faunadb import query as q
        from faunadb.objects import Ref
        from faunadb.client import FaunaClient
        client = FaunaClient(
        secret=os.getenv('fauna_secret'),domain="db.fauna.com",port=443,scheme="https")

        # create obj referencing 'car_objects' collection in 'car_ratebooks' db in FaunaDB
        query = client.query( # query starts with 'data' and contains array of objects that start with 'ref', then 'data' again
            q.map_(q.lambda_(["X"], q.get(q.var("X"))),
                q.paginate(q.documents(q.collection('car_objects')),size=100000)))

        # create a list of fauna caps and ref ids for easier iteration
        caps_refs = []
        for car in query['data']:
            caps_refs.append([car['data']['cap_code'],car['ref'].id()])

        # create a list containing fauna caps for easier iteration
        fauna_caps = []
        for car in query['data']:
            fauna_caps.append(car['data']['cap_code'])

        ### create list of cars not in fauna, and matching cars from new rb
        new_cars_rb = []
        same_cars_rb = []
        for car in cars_list:
            if car['cap_code'] not in fauna_caps:
                new_cars_rb.append(car)
            else:
                same_cars_rb.append(car)

        # create list of caps in same_cars
        same_cars_caps = []
        for car in same_cars_rb:
            same_cars_caps.append(car['cap_code'])

        ## create list of fauna objs only if there is a respective cap in new rb
        same_cars_fauna = []
        for car in query['data']:
            if car['data']['cap_code'] in same_cars_caps:
                same_cars_fauna.append(car)
                
        ### create a list of fauna objs without the ref part; to easily compare with new rb rates
        same_cars_fauna_no_refs = []
        for car in same_cars_fauna:
            same_cars_fauna_no_refs.append(car['data'])

        ### sort same_cars_fauna_no_refs by cap_code
        from operator import itemgetter
        same_cars_fauna_no_refs_sorted = sorted(same_cars_fauna_no_refs, key=itemgetter('cap_code'))
        same_cars_rb_sorted = sorted(same_cars_rb, key=itemgetter('cap_code'))

        ### get list of unmatching prices cap_codes
        unmatched_prices_caps = []

        idx = 0
        for car in same_cars_fauna_no_refs_sorted:
            try: # if there is a lender_name already
                if car['prices'][lender_name] != same_cars_rb_sorted[idx]['prices'][lender_name]:
                    unmatched_prices_caps.append(car['cap_code'])
                    print(car['cap_code'],"is unmatched!")
            except: # if there is no lender_name already
                unmatched_prices_caps.append(car['cap_code'])
                print(lender_name,"does not already exist!")
            idx+=1

        # initiate refresh variable to hold False in case no refresh is required after price checks
        refresh = False

        # initiate updated cars list
        updated_cars = []
        
        # for car in same_cars_rb; if car cap code is in unmatched_prices_caps - then get that car's prices from same_cars_rb,
        # and update fauna accordingly
        for car in same_cars_rb:
            if car['cap_code'] in unmatched_prices_caps:
                for pair in caps_refs:
                    if pair[0] == car['cap_code']:
                        ref = pair[1] # assign relevant ref
                new_rates = car['prices'][lender_name]
                # update the respective prices: lender_name
                client.query(q.update(q.ref(q.collection('car_objects'),ref),
                {'data':{'prices':{lender_name:new_rates}}}))
                updated_cars.append(car['cap_code'])
                refresh = True

        ### finally - clear the cache, if there is a need to

        if refresh:

            import urllib.request
            webURL = urllib.request.urlopen(os.getenv('cache_url'))
            if webURL.getcode() == 200:
                print("refresh was successful!")
            else:
                print("something went wrong with refresh!")

        if len(updated_cars) > 0:
            if len(updated_cars) == 1:
                print(f"The following car's rates were updated in Fauna: {updated_cars}")
            else:
                print(f"The following {len(updated_cars)} cars' rates were updated in Fauna: {updated_cars}")
        else:
            print("No cars needed updating!")

    else: # if no file was found
        print("No file was found!")
        # what time and day is it now?
        now = time.localtime()
        if now.tm_wday == 0 and now.tm_hour > 10: # if it's monday after 10am
            slack_info = f"No file was found on the shared Google Drive! Check if it's available here: [Google Drive folder link]"
            post_message_to_slack(slack_info)
            print(slack_info)

    ### END OF JOB ###

# run script every hour at 00
schedule.every().hour.at(":00").do(job)
while True:
    schedule.run_pending()
    time.sleep(1)
