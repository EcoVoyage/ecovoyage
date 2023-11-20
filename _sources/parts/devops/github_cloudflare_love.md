# When GitHub Pages Met Cloudflare

## A Love Story Written in DNS Records

> Can you write me a script for linux to add the DNS records to the domain atrawog.org. needed to run a github page for the domain? Using the Cloudflare API and an API Tokens called CLOUDFLARE_TOKEN and please include instructions for Arch Linux too.  

### Setting up DNS records for a GitHub Pages site using Cloudflare API on Linux

To create a script that adds the necessary DNS records for your GitHub Pages site to your atrawog.org domain using the Cloudflare API, follow the steps below.

1. Install `curl` and `jq` on your Linux system (if not already installed) using the package manager:

For Ubuntu / Debian-based systems:

```bash
sudo apt-get update
sudo apt-get install curl jq
```

For CentOS / RHEL-based systems:

```bash
sudo yum install epel-release
sudo yum update
sudo yum install curl jq
```

For Arch Linux systems:

```bash
sudo pacman -Sy
sudo pacman -S curl jq
```

2. Create a new shell script file called `add_dns_records.sh`:

```bash
touch add_dns_records.sh
chmod +x add_dns_records.sh
```

3. Open the `add_dns_records.sh` file in your favorite text editor and add the following script:

```bash
#!/bin/bash

# Set the Cloudflare API Token
CLOUDFLARE_TOKEN="your_cloudflare_token_here"

# Set the domain, GitHub Pages IP addresses, and hostname
DOMAIN="atrawog.org"
GITHUB_PAGES_IPV4=("185.199.108.153" "185.199.109.153" "185.199.110.153" "185.199.111.153")
HOSTNAME="www"

# Get the Zone ID for the domain
ZONE_ID=$(curl -s -X GET "https://api.cloudflare.com/client/v4/zones?name=$DOMAIN" \
  -H "Authorization: Bearer $CLOUDFLARE_TOKEN" \
  -H "Content-Type: application/json" | jq -r '.result[0].id')

# Add A records for the domain
for ip in "${GITHUB_PAGES_IPV4[@]}"; do
  curl -s -X POST "https://api.cloudflare.com/client/v4/zones/$ZONE_ID/dns_records" \
    -H "Authorization: Bearer $CLOUDFLARE_TOKEN" \
    -H "Content-Type: application/json" \
    --data '{"type":"A","name":"'$DOMAIN'","content":"'$ip'","ttl":1,"proxied":false}'
done

# Add CNAME record for the www subdomain
curl -s -X POST "https://api.cloudflare.com/client/v4/zones/$ZONE_ID/dns_records" \
  -H "Authorization: Bearer $CLOUDFLARE_TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"type":"CNAME","name":"'$HOSTNAME'","content":"'$DOMAIN'","ttl":1,"proxied":false}'

echo "DNS records added successfully."
```

4. Replace `"your_cloudflare_token_here"` with your actual Cloudflare API Token. Make sure to keep the double-quotes around the token.

5. Save the file and exit the text editor.

6. Run the script:

```bash
./add_dns_records.sh
```

This script will add the necessary DNS records to your atrawog.org domain using the Cloudflare API. Ensure that you have the correct API Token and domain name in the script.

Please note that it may take some time for the DNS changes to propagate. Once the DNS records have been updated, your GitHub Pages site should be accessible via your custom domain.
