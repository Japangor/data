# Security Setup Guide

## Firebase Credentials Configuration

Your Firebase service account credentials have been moved to environment variables for better security. Here's how to set them up properly:

## 1. Local Development

### Option A: Using .env file (Recommended)
1. Copy `.env.example` to `.env`
2. Fill in your Firebase credentials in the `.env` file
3. The application will automatically load these variables

### Option B: System Environment Variables
Set these environment variables in your system:

```bash
export FCM_PROJECT_ID="your-firebase-project-id"
export FCM_PRIVATE_KEY_ID="your-private-key-id"
export FCM_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\nYOUR_PRIVATE_KEY_HERE\n-----END PRIVATE KEY-----"
export FCM_CLIENT_EMAIL="firebase-adminsdk-xxxxx@your-project.iam.gserviceaccount.com"
export FCM_CLIENT_ID="your-client-id"
export FCM_CLIENT_X509_CERT_URL="https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-xxxxx%40your-project.iam.gserviceaccount.com"
```

## 2. Production Server Setup

### For Linux/Ubuntu Server:

1. **Create environment file:**
```bash
sudo nano /etc/environment
```

2. **Add Firebase variables:**
```bash
FCM_PROJECT_ID="cricket-c7b8f"
FCM_PRIVATE_KEY_ID="61efb46d525b2b26358d4a9e2418ff55b0073340"
FCM_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\nMIIEugIBADANBgkqhkiG9w0BAQEFAASCBKQwggSgAgEAAoIBAQChQioJ6RhFbA6J\nsS+F+TAHsJkYPz7nKI0Wdmu5lTamXIBtMVv045pAOZIXiWRqUXD/rA8+DOHip851\n0pdZn3Jk7X5ndKR9vnwh41c+kfLnFXo+VlFZh1FrIvWbbz0UfOWU7LyY1oRCqdXO\nv2EJK8Cu5zjOjlTU5+dgw2/zMdbR+/yf6gBOlm9PmhxnnuFiLp3AyemQesQwjepj\n3j7/vQsgWgwt8gqlqGJTfCwUctTngvvpjSlQanqZjfErRkjPLmWOWRiELOVV+6SY\ns6wubSdks4nHBuF1pvoO5PIAnRf4X90JTK4LM3DfcdqMBLeJONtseNXFGXx+Yvlj\nvDW8zs9pAgMBAAECggEABAUkCsBoRErJh5gGnEUDDbefiW6bzdcOW8Uyy4FmlGBX\n+ODmVV7BmK19I9/fKsgolGcgr7/rqnyoDy3ja6H0j+mPx8DgdoLW+C1OHn3OKJtt\nECLGWM06M8m/H7ndM8Am8dh/alWZ6uOHzf8QlziJz186Sx2NpdhS+O/5jLGGyBAa\n+zHXyARir8MOQk6rcOuVyM7Go4aEfTEmJ1qE8Zyvh33Ir/n9S3to1EstttF0DfMO\ng+eVHw+ZY7hETzsqoHZfRPsEAhj++oApwhBAVj4ooCcUyLB4ed9oFoCYllyHFiQS\n0fjmkur0jGA/SxD3bApC4jc1G7KVSs3ria6eD6bFlQKBgQDVx9EP3xw+WKX4NhSz\nNaVNztpdkyJtU/LCCRSpYKdXDfdf2+OXlMo0Pp9lUzoFgieXlZ3oAVdT5CsYt2sP\ngPU0EvzQRm3++krdhICCGqNOMVHt7dRHNMP1na5nrhPgHK8/ULh+MP1Ol8jaJM3f\nlX5Y+rwu6ANHUOy6DeRAEeNHQwKBgQDBGvgJv0TefcdbKof+qssC+ueUZ1ZEyz0m\n9a395MadkyKvLbE9N02hu9x+PTs+POngJnqsel1yFs6cvQTE2V0+HlYEME0RftRe\nT9Wm2bRG9dEWgooijR6AxjhH/I7R97ZbZxTGbbL5XQCEiaUzvXAoDbEZm1tPQLOV\nu9sSLKd14wJ/FAqnjtVb0Hx/EsX/Ks3csW6zChLBJ363Q3mWdkqZZY/poRL8Qzmv\nuzYju1wSFHIfRBbCtv1pMnQxlh+b6dgtJLSi/uObbEwKuLmzUn8s0CfPhn7FZ+eK\nd3xQ3Wd0dY4637SxlyMAnF2edtT8d/mL3sui2MLni8gKaWbj/x2uMQKBgFuGQsWr\n6CJkDxcAo5BqtunkrdpC2Dqm8YcDrsHFqvWIhUnuKDFRgvQDLFCRCQFIsbjRxBb3\nsE6gzLCxTg9WzsDgc/hsRDrkmBdTU3pPeQig/cbjfEFADZMPYCGObMrL05yi0M/z\nsn5KPDKVYr2RLhVt1+DBJ5f6nKN9dCm7DGvtAoGAQBV4shfHkKen/9ePjQzfaBQK\nm8Na3CwxW6rtFkkvztKZQ/3e7KkeBikdxDOYnyOjyNPwqOFpit6G5C9bQT2DvCGZ\n/vClWoa7KCI0iGKubDaa4nlrCs2j1qiJSF6Qtgg9+ABmzAr9lv9XwO6tURYnah6o\nmYBwozip0iZzAzGDCRQ=\n-----END PRIVATE KEY-----"
FCM_CLIENT_EMAIL="firebase-adminsdk-fbsvc@cricket-c7b8f.iam.gserviceaccount.com"
FCM_CLIENT_ID="107021160527779461828"
FCM_CLIENT_X509_CERT_URL="https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-fbsvc%40cricket-c7b8f.iam.gserviceaccount.com"
```

3. **Reload environment:**
```bash
source /etc/environment
```

### For Docker Production:

1. **Create a secure .env file on your server:**
```bash
nano /path/to/your/app/.env
```

2. **Set proper permissions:**
```bash
chmod 600 .env
chown root:root .env
```

### For Cloud Providers (AWS, GCP, Azure):

#### AWS EC2:
- Use AWS Systems Manager Parameter Store or AWS Secrets Manager
- Set environment variables in your deployment configuration

#### Google Cloud:
- Use Google Secret Manager
- Set environment variables in your Cloud Run or Compute Engine configuration

#### Azure:
- Use Azure Key Vault
- Set environment variables in your App Service configuration

## 3. Security Best Practices

### ✅ DO:
- Use environment variables for all sensitive data
- Set proper file permissions (600) for .env files
- Use cloud-native secret management services in production
- Rotate credentials regularly
- Monitor access to your Firebase project

### ❌ DON'T:
- Commit .env files to version control
- Store credentials in code
- Share credentials via email or chat
- Use the same credentials across environments
- Leave default permissions on credential files

## 4. Verification

To verify your setup is working:

1. **Check environment variables are loaded:**
```bash
echo $FCM_PROJECT_ID
```

2. **Test the application:**
- Start your application
- Check logs for FCM initialization
- Test sending a notification

3. **Monitor for errors:**
- Check application logs for authentication errors
- Verify Firebase console for API usage

## 5. Troubleshooting

### Common Issues:

1. **"Missing required Firebase environment variables"**
   - Ensure all FCM_* variables are set
   - Check for typos in variable names

2. **"Invalid private key format"**
   - Ensure private key includes proper newlines
   - Check for escaped characters (\n)

3. **"Authentication failed"**
   - Verify credentials are correct
   - Check Firebase project permissions

### Debug Commands:
```bash
# Check if variables are set
env | grep FCM

# Test Docker environment
docker-compose exec consumer env | grep FCM
```

## 6. Migration Checklist

- [x] Move credentials from JSON file to environment variables
- [x] Update application code to use environment variables
- [x] Update Docker configuration
- [x] Delete sensitive JSON files
- [x] Test application functionality
- [ ] Deploy to production with secure environment setup
- [ ] Verify notifications are working
- [ ] Monitor logs for any issues

## Support

If you encounter issues:
1. Check the application logs
2. Verify all environment variables are set correctly
3. Test with a simple FCM notification
4. Check Firebase console for any project-level issues