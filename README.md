# Momment-Grabber-Backend
NYU Tondon CS-GY9223 Cloud Computing Final Project
### Momment Grabber is a photo sharing mobile application. Sometimes people come into other's photos when they are not aware of it. By using this APP people can search for photos where they show up. For details, please check the final report of this project: https://docs.google.com/document/d/1X5T3kvnGgvXfyT6ZBAQfABO8peGpuOvWweIRtEH49SY/edit?usp=sharing       

##This is the backend codes of the App 

- Dynamo.java: processes searching requests, searches the DynamoDB for photos according to the time and location given by users. We use GeoPoint API to support geolocation searches.
- App.java: Define functions that leverage MicroSoft Face API to perform face recognition related operations. 
- AddToFaceList.java: Every time a user uploads new selfies, this program record them in user's face list, so that latter we can perform Face-Matching. 
- FaceMatch.java: Identifies whether a user shows up in a found photo. If so, send the url of the photo through Google Cloud Message to user's cell phone, so that he/she can download it and see how does he/she look like in other's photo.
