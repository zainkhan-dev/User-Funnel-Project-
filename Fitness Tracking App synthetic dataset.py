import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import random
from faker import Faker
import os, zipfile

start_date = datetime(2024,1,1)
end_date   = datetime(2024,7,31)
outdir     = "fitness_synthetic"
os.makedirs(outdir, exist_ok=True)

faker = Faker()
random.seed(42)
np.random.seed(42)

days = (end_date - start_date).days + 1
dates = [start_date + timedelta(days=i) for i in range(days)]
daily_installs = np.round(np.linspace(500, 1200, days)).astype(int)

users = []
user_id = 1000000

channels = ['organic', 'social', 'paid_search', 'referral', 'email']
devices  = ['iOS','Android']
genders  = ['male','female','other']
countries= ['Pakistan','USA','UK','Canada','Australia','Germany']

base_segment_probs = {'casual':0.60,'committed':0.30,'enthusiast':0.10}

for day, installs in zip(dates, daily_installs):
    for _ in range(installs):
        user_id += 1
        age = int(np.clip(np.random.normal(32, 8), 16, 65))
        gender = random.choices(genders, [0.48,0.48,0.04])[0]
        channel= random.choices(channels, [0.40,0.30,0.18,0.08,0.04])[0]
        device = random.choice(devices)
        country= random.choices(countries, [0.4,0.2,0.12,0.08,0.12,0.08])[0]
        
        seg_probs = base_segment_probs.copy()
        if channel=='social': seg_probs['casual']+=0.05; seg_probs['enthusiast']-=0.02
        if channel=='paid_search': seg_probs['committed']+=0.04; seg_probs['casual']-=0.02
        if channel=='referral': seg_probs['enthusiast']+=0.05; seg_probs['casual']-=0.03
        total = sum(seg_probs.values())
        seg_probs = {k:v/total for k,v in seg_probs.items()}
        segment = random.choices(list(seg_probs.keys()), weights=list(seg_probs.values()))[0]

        users.append({
            "user_id": f"U{user_id}",
            "install_date": day + timedelta(seconds=random.randint(0,86399)),
            "age": age,
            "gender": gender,
            "acquisition_channel": channel,
            "device_type": device,
            "country": country,
            "segment": segment
        })

users_df = pd.DataFrame(users)


def after(dt, min_hours=0, max_days=7):
    return dt + timedelta(seconds=random.randint(min_hours*3600, max_days*24*3600))

conversion_probs = {
    'casual':    {'create_account':0.75,'profile_setup':0.50,'first_workout':0.35,'week1_retention':0.20,'premium':0.02},
    'committed': {'create_account':0.90,'profile_setup':0.80,'first_workout':0.70,'week1_retention':0.45,'premium':0.10},
    'enthusiast':{'create_account':0.98,'profile_setup':0.95,'first_workout':0.90,'week1_retention':0.75,'premium':0.35},
}

events, sessions, subs = [], [], []
sess_id = 5000000

for _, u in users_df.iterrows():
    uid, seg, inst = u["user_id"], u["segment"], u["install_date"]
    
    # Funnel events
    if random.random() < conversion_probs[seg]['create_account']:
        acct = after(inst,1,3)
        events.append({"user_id":uid,"event_type":"account_created","event_timestamp":acct})
        
        if random.random() < conversion_probs[seg]['profile_setup']:
            prof = after(acct,0,7)
            events.append({"user_id":uid,"event_type":"profile_setup","event_timestamp":prof})
            
            if random.random() < conversion_probs[seg]['first_workout']:
                fw = after(prof,0,14)
                workout_type = random.choice(['run','bike','yoga','strength','hiit','walk'])
                dur = int(np.clip(np.random.normal(35 if seg!="casual" else 20,10),5,180))
                cal = int(dur*np.random.uniform(6,10))
                events.append({"user_id":uid,"event_type":"first_workout","event_timestamp":fw,
                               "workout_type":workout_type,"duration_mins":dur,"calories":cal})
    
    # Week 1 return
    if random.random() < conversion_probs[seg]['week1_retention']:
        week1 = inst + timedelta(days=random.randint(7,14), seconds=random.randint(0,86399))
        sess_id += 1
        sessions.append({"user_id":uid,"session_id":f"S{sess_id}","session_start":week1,
                         "session_duration_mins":int(np.clip(np.random.exponential(25),3,180))})
        events.append({"user_id":uid,"event_type":"week1_return","event_timestamp":week1})
    
    # Sessions (30 days)
    mean_sessions = 2 if seg=="casual" else 8 if seg=="committed" else 18
    for _ in range(np.random.poisson(mean_sessions)):
        offset = random.randint(0,29)
        sess = inst + timedelta(days=offset, seconds=random.randint(0,86399))
        sess_id += 1
        sessions.append({"user_id":uid,"session_id":f"S{sess_id}","session_start":sess,
                         "session_duration_mins":int(np.clip(np.random.exponential(30),3,240))})
    
    # Subscription
    if random.random() < conversion_probs[seg]['premium']:
        sub_date = inst + timedelta(days=int(np.clip(np.random.exponential(14),1,90)))
        plan = random.choices(['monthly','yearly'], [0.75,0.25])[0]
        price = 9.99 if plan=="monthly" else 79.99
        subs.append({"user_id":uid,"subscription_date":sub_date,"plan":plan,"price_usd":price})


events_df = pd.DataFrame(events)
sessions_df = pd.DataFrame(sessions)
subs_df = pd.DataFrame(subs)

users_df.to_csv(f"{outdir}/users.csv", index=False)
events_df.to_csv(f"{outdir}/events.csv", index=False)
sessions_df.to_csv(f"{outdir}/sessions.csv", index=False)
subs_df.to_csv(f"{outdir}/subscriptions.csv", index=False)

# Optionally zip everything
with zipfile.ZipFile("fitness_synthetic.zip","w") as zf:
    for fname in ["users.csv","events.csv","sessions.csv","subscriptions.csv"]:
        zf.write(f"{outdir}/{fname}", arcname=fname)

print("✅ Data generated!")
print(f"Users: {len(users_df):,}")
print(f"Events: {len(events_df):,}")
print(f"Sessions: {len(sessions_df):,}")
print(f"Subscriptions: {len(subs_df):,}")
print("All CSVs saved inside 'fitness_synthetic/' and zipped as fitness_synthetic.zip")