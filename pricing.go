package main

// listCostPer1000 is S3 PUT/COPY/POST/LIST price per 1,000 requests by region.
// Source: https://aws.amazon.com/s3/pricing/ (as of 2025)
// Default (unlisted regions): $0.005
var listCostPer1000 = map[string]float64{
	// US / Canada
	"us-east-1":      0.005,
	"us-east-2":      0.005,
	"us-west-1":      0.005,
	"us-west-2":      0.005,
	"ca-central-1":   0.005,
	"ca-west-1":      0.005,
	// Europe
	"eu-west-1":      0.005,
	"eu-west-2":      0.005,
	"eu-west-3":      0.005,
	"eu-central-1":   0.005,
	"eu-central-2":   0.005,
	"eu-north-1":     0.005,
	"eu-south-1":     0.005,
	"eu-south-2":     0.005,
	// Asia Pacific
	"ap-northeast-1": 0.005,
	"ap-northeast-2": 0.005,
	"ap-northeast-3": 0.005,
	"ap-southeast-1": 0.005,
	"ap-southeast-2": 0.005,
	"ap-southeast-3": 0.005,
	"ap-southeast-4": 0.005,
	"ap-south-1":     0.005,
	"ap-south-2":     0.005,
	"ap-east-1":      0.005,
	// Middle East / Africa / South America — higher pricing
	"me-south-1":     0.006,
	"me-central-1":   0.006,
	"af-south-1":     0.007,
	"sa-east-1":      0.007,
	// GovCloud
	"us-gov-east-1":  0.005,
	"us-gov-west-1":  0.005,
}

func listCostForRegion(region string) float64 {
	if p, ok := listCostPer1000[region]; ok {
		return p
	}
	return 0.005
}

func computeCost(listRequests int64, region string) float64 {
	pricePerReq := listCostForRegion(region) / 1000.0
	return float64(listRequests) * pricePerReq
}
