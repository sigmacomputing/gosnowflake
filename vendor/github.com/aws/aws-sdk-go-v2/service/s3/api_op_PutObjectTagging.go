// Code generated by smithy-go-codegen DO NOT EDIT.

package s3

import (
	"context"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	s3cust "github.com/aws/aws-sdk-go-v2/service/s3/internal/customizations"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Sets the supplied tag-set to an object that already exists in a bucket. A tag is
// a key-value pair. You can associate tags with an object by sending a PUT request
// against the tagging subresource that is associated with the object. You can
// retrieve tags by sending a GET request. For more information, see
// GetObjectTagging
// (https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html). For
// tagging-related restrictions related to characters and encodings, see Tag
// Restrictions
// (https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/allocation-tag-restrictions.html).
// Note that Amazon S3 limits the maximum number of tags to 10 tags per object. To
// use this operation, you must have permission to perform the s3:PutObjectTagging
// action. By default, the bucket owner has this permission and can grant this
// permission to others. To put tags of any other version, use the versionId query
// parameter. You also need permission for the s3:PutObjectVersionTagging action.
// For information about the Amazon S3 object tagging feature, see Object Tagging
// (https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html). Special
// Errors
//
// * Code: InvalidTagError
//
// * Cause: The tag provided was not a valid tag.
// This error can occur if the tag did not pass input validation. For more
// information, see Object Tagging
// (https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html).
//
// * Code:
// MalformedXMLError
//
// * Cause: The XML provided does not match the schema.
//
// * Code:
// OperationAbortedError
//
// * Cause: A conflicting conditional action is currently in
// progress against this resource. Please try again.
//
// * Code: InternalError
//
// *
// Cause: The service was unable to apply the provided tag to the object.
//
// Related
// Resources
//
// * GetObjectTagging
// (https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html)
//
// *
// DeleteObjectTagging
// (https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html)
func (c *Client) PutObjectTagging(ctx context.Context, params *PutObjectTaggingInput, optFns ...func(*Options)) (*PutObjectTaggingOutput, error) {
	if params == nil {
		params = &PutObjectTaggingInput{}
	}

	result, metadata, err := c.invokeOperation(ctx, "PutObjectTagging", params, optFns, addOperationPutObjectTaggingMiddlewares)
	if err != nil {
		return nil, err
	}

	out := result.(*PutObjectTaggingOutput)
	out.ResultMetadata = metadata
	return out, nil
}

type PutObjectTaggingInput struct {

	// The bucket name containing the object. When using this action with an access
	// point, you must direct requests to the access point hostname. The access point
	// hostname takes the form
	// AccessPointName-AccountId.s3-accesspoint.Region.amazonaws.com. When using this
	// action with an access point through the AWS SDKs, you provide the access point
	// ARN in place of the bucket name. For more information about access point ARNs,
	// see Using Access Points
	// (https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html)
	// in the Amazon S3 User Guide. When using this action with Amazon S3 on Outposts,
	// you must direct requests to the S3 on Outposts hostname. The S3 on Outposts
	// hostname takes the form
	// AccessPointName-AccountId.outpostID.s3-outposts.Region.amazonaws.com. When using
	// this action using S3 on Outposts through the AWS SDKs, you provide the Outposts
	// bucket ARN in place of the bucket name. For more information about S3 on
	// Outposts ARNs, see Using S3 on Outposts
	// (https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html) in the
	// Amazon S3 User Guide.
	//
	// This member is required.
	Bucket *string

	// Name of the object key.
	//
	// This member is required.
	Key *string

	// Container for the TagSet and Tag elements
	//
	// This member is required.
	Tagging *types.Tagging

	// The MD5 hash for the request body. For requests made using the AWS Command Line
	// Interface (CLI) or AWS SDKs, this field is calculated automatically.
	ContentMD5 *string

	// The account ID of the expected bucket owner. If the bucket is owned by a
	// different account, the request will fail with an HTTP 403 (Access Denied) error.
	ExpectedBucketOwner *string

	// Confirms that the requester knows that they will be charged for the request.
	// Bucket owners need not specify this parameter in their requests. For information
	// about downloading objects from requester pays buckets, see Downloading Objects
	// in Requestor Pays Buckets
	// (https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectsinRequesterPaysBuckets.html)
	// in the Amazon S3 Developer Guide.
	RequestPayer types.RequestPayer

	// The versionId of the object that the tag-set will be added to.
	VersionId *string
}

type PutObjectTaggingOutput struct {

	// The versionId of the object the tag-set was added to.
	VersionId *string

	// Metadata pertaining to the operation's result.
	ResultMetadata middleware.Metadata
}

func addOperationPutObjectTaggingMiddlewares(stack *middleware.Stack, options Options) (err error) {
	err = stack.Serialize.Add(&awsRestxml_serializeOpPutObjectTagging{}, middleware.After)
	if err != nil {
		return err
	}
	err = stack.Deserialize.Add(&awsRestxml_deserializeOpPutObjectTagging{}, middleware.After)
	if err != nil {
		return err
	}
	if err = addSetLoggerMiddleware(stack, options); err != nil {
		return err
	}
	if err = awsmiddleware.AddClientRequestIDMiddleware(stack); err != nil {
		return err
	}
	if err = smithyhttp.AddComputeContentLengthMiddleware(stack); err != nil {
		return err
	}
	if err = addResolveEndpointMiddleware(stack, options); err != nil {
		return err
	}
	if err = v4.AddComputePayloadSHA256Middleware(stack); err != nil {
		return err
	}
	if err = addRetryMiddlewares(stack, options); err != nil {
		return err
	}
	if err = addHTTPSignerV4Middleware(stack, options); err != nil {
		return err
	}
	if err = awsmiddleware.AddRawResponseToMetadata(stack); err != nil {
		return err
	}
	if err = awsmiddleware.AddRecordResponseTiming(stack); err != nil {
		return err
	}
	if err = addClientUserAgent(stack); err != nil {
		return err
	}
	if err = smithyhttp.AddErrorCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = smithyhttp.AddCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = addOpPutObjectTaggingValidationMiddleware(stack); err != nil {
		return err
	}
	if err = stack.Initialize.Add(newServiceMetadataMiddleware_opPutObjectTagging(options.Region), middleware.Before); err != nil {
		return err
	}
	if err = addMetadataRetrieverMiddleware(stack); err != nil {
		return err
	}
	if err = addPutObjectTaggingUpdateEndpoint(stack, options); err != nil {
		return err
	}
	if err = addResponseErrorMiddleware(stack); err != nil {
		return err
	}
	if err = v4.AddContentSHA256HeaderMiddleware(stack); err != nil {
		return err
	}
	if err = disableAcceptEncodingGzip(stack); err != nil {
		return err
	}
	if err = addRequestResponseLogging(stack, options); err != nil {
		return err
	}
	if err = smithyhttp.AddContentChecksumMiddleware(stack); err != nil {
		return err
	}
	return nil
}

func newServiceMetadataMiddleware_opPutObjectTagging(region string) *awsmiddleware.RegisterServiceMetadata {
	return &awsmiddleware.RegisterServiceMetadata{
		Region:        region,
		ServiceID:     ServiceID,
		SigningName:   "s3",
		OperationName: "PutObjectTagging",
	}
}

// getPutObjectTaggingBucketMember returns a pointer to string denoting a provided
// bucket member valueand a boolean indicating if the input has a modeled bucket
// name,
func getPutObjectTaggingBucketMember(input interface{}) (*string, bool) {
	in := input.(*PutObjectTaggingInput)
	if in.Bucket == nil {
		return nil, false
	}
	return in.Bucket, true
}
func addPutObjectTaggingUpdateEndpoint(stack *middleware.Stack, options Options) error {
	return s3cust.UpdateEndpoint(stack, s3cust.UpdateEndpointOptions{
		Accessor: s3cust.UpdateEndpointParameterAccessor{
			GetBucketFromInput: getPutObjectTaggingBucketMember,
		},
		UsePathStyle:            options.UsePathStyle,
		UseAccelerate:           options.UseAccelerate,
		SupportsAccelerate:      true,
		TargetS3ObjectLambda:    false,
		EndpointResolver:        options.EndpointResolver,
		EndpointResolverOptions: options.EndpointOptions,
		UseDualstack:            options.UseDualstack,
		UseARNRegion:            options.UseARNRegion,
	})
}
