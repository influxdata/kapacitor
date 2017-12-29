package client

import (
	"fmt"
	//"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/pkg/errors"
)

type Config struct {
	Region    string
	AccessKey string
	SecretKey string
}

type Client interface {
	Update(c Config) error
	Version() (string, error)
	Group(id string) (*autoscaling.DescribeAutoScalingGroupsOutput, error)
	UpdateGroup(id string, desiredcapacity int64) error
}

type autoScaling struct {
	client *autoscaling.AutoScaling
}

func New(c Config) (Client, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(c.Region),
		Credentials: credentials.NewStaticCredentials(c.AccessKey, c.SecretKey, ""),
	})
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	svc := autoscaling.New(sess)
	return &autoScaling{
		client: svc,
	}, nil
}

func (svc *autoScaling) Update(new Config) error {

	return nil
}

func (svc *autoScaling) Version() (string, error) {
	return "2013-10-15", nil
}

func (svc *autoScaling) Group(id string) (*autoscaling.DescribeAutoScalingGroupsOutput, error) {

	input := &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []*string{
			aws.String(id),
		},
	}

	result, err := svc.client.DescribeAutoScalingGroups(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case autoscaling.ErrCodeInvalidNextToken:
				return nil, errors.Wrapf(err, "failed to create GET request for %q", autoscaling.ErrCodeInvalidNextToken)
			case autoscaling.ErrCodeResourceContentionFault:
				return nil, errors.Wrapf(err, "failed to create GET request for %q", autoscaling.ErrCodeResourceContentionFault)
			default:
				return nil, errors.Wrapf(err, "failed to create GET request for %s", aerr.Error())
			}
		} else {
			return nil, errors.Wrapf(err, "failed to create GET request for %s", err.Error())
		}
	}

	return result, nil
}

func (svc *autoScaling) UpdateGroup(id string, desiredcapacity int64) error {

	input := &autoscaling.SetDesiredCapacityInput{
		AutoScalingGroupName: aws.String(id),
		DesiredCapacity:      aws.Int64(desiredcapacity),
		HonorCooldown:        aws.Bool(false), //Since kapacitor has the cooldown implementation, Aws implementation is explicitly set to false
	}

	_, err := svc.client.SetDesiredCapacity(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case autoscaling.ErrCodeScalingActivityInProgressFault:
				return errors.Wrapf(err, "Failed to set desired capacity %q", autoscaling.ErrCodeScalingActivityInProgressFault)

			case autoscaling.ErrCodeResourceContentionFault:
				return errors.Wrapf(err, "Failed to set desired capacity %q", autoscaling.ErrCodeResourceContentionFault)

			default:
				return errors.Wrapf(err, "Failed to set desired capacity %s", aerr.Error())
			}
		} else {
			return errors.Wrapf(err, "Failed to set desired capacity %s", err.Error())
		}
	}

	return nil
}
