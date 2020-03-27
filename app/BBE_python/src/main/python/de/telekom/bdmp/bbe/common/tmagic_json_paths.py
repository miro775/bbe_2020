
FAC_v2_klsid_ps = 'json_data.availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].service.place[0].id'
FAC_v2_eventid  = 'json_data.availabilityCheckCalledEvent.eventId'
# temporary,  only date, truncated HH:MM:ss  because extra char "T"
# F.to_timestamp(F.col('json_data.availabilityCheckCalledEvent.eventTime')[0:10],'yyyy-MM-dd').alias('requesttime_ISO'),
FAC_v2_eventTime =  'json_data.availabilityCheckCalledEvent.eventTime'
FAC_v2_partyid = 'json_data.availabilityCheckCalledEvent.partyId'
FAC_v2_eligibilityUnavailabilityReasonCode  = 'json_data.availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].eligibilityUnavailabilityReason[0].code'
FAC_v2_eligibilityUnavailabilityReasonLabel = 'json_data.availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].eligibilityUnavailabilityReason[0].label'

# address_type
FAC_v2__place  = 'json_data.availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].service.place[0]'

# ausbaustandgf
FAC_v2__serviceCharacteristic = 'json_data.availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].service.serviceCharacteristic[0]'
