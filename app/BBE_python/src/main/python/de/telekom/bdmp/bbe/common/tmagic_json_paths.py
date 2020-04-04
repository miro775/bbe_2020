
FAC_v2_klsid_ps = 'json_data.availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].service.place[0].id'
FAC_v2_eventid  = 'json_data.availabilityCheckCalledEvent.eventId'
# temporary,  only date, truncated HH:MM:ss  because extra char "T"
# F.to_timestamp(F.col('json_data.availabilityCheckCalledEvent.eventTime')[0:10],'yyyy-MM-dd').alias('requesttime_ISO'),
FAC_v2_eventTime =  'json_data.availabilityCheckCalledEvent.eventTime'
FAC_v2_partyid = 'json_data.availabilityCheckCalledEvent.partyId'
FAC_v2_eligibilityUnavailabilityReasonCode  = 'json_data.availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].eligibilityUnavailabilityReason[0].code'
FAC_v2_eligibilityUnavailabilityReasonLabel = 'json_data.availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].eligibilityUnavailabilityReason[0].label'

# address_type
FAC_v2__place_0  = 'json_data.availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].service.place[0]'
#FAC_v2__place_struct = '$.availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].service.place[0]'

# serviceCharacteristic
#  varianta 1 : syntax for F.expr() ,  dtype will be :  array<struct.....
FAC_v2__json_serviceCharacteristic_x1 = "json_data.availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].service.serviceCharacteristic"
# varianta 2: syntax for F.get_json_object()  ,  dtype will be:  string
FAC_v2__json_serviceCharacteristic_x2 = "$.availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].service.serviceCharacteristic"

#  service.serviceCharacteristic -  this struct contains fields with "@" in name !!
#  character @ is problem!,,   array<struct<@baseType:string,@schemaLocation:string,@type:string,name:string,value:string,valueType:string>>


FAC_v2__serviceCharacteristic0_name ="json_data.availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].service.serviceCharacteristic[0].name"
FAC_v2__serviceCharacteristic0_value ="json_data.availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].service.serviceCharacteristic[0].value"