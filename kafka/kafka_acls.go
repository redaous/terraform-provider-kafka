package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

type ACL struct {
	Principal      string `json:"principal"`
	Host           string `json:"host"`
	Operation      string `json:"operation"`
	PermissionType string `json:"permission_type"`
}

type Resource struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

const unknownConversion = -1

func (c *Client) DeleteACLPrincipal(principal string) error {
	f := sarama.AclFilter{Principal: &principal}
	return c.DeleteACL(f)
}

func (c *Client) DeleteACL(filter sarama.AclFilter) error {
	broker, err := c.availableBroker()
	if err != nil {
		return err
	}
	req := &sarama.DeleteAclsRequest{
		Filters: []*sarama.AclFilter{&filter},
	}
	//log.Printf("[INFO] Deleting ACL %v\n", s)

	res, err := broker.DeleteAcls(req)
	if err != nil {
		return err
	}

	for _, r := range res.FilterResponses {
		if r.Err != sarama.ErrNoError {
			return r.Err
		}
	}
	return nil
}

func (c *Client) CreateACL(ac *sarama.AclCreation) error {
	broker, err := c.availableBroker()
	if err != nil {
		return err
	}

	req := &sarama.CreateAclsRequest{
		AclCreations: []*sarama.AclCreation{ac},
	}

	res, err := broker.CreateAcls(req)
	if err != nil {
		return err
	}

	for _, r := range res.AclCreationResponses {
		if r.Err != sarama.ErrNoError {
			return r.Err
		}
	}

	return nil
}

// ListACLs lists all the known ACLs
func (c *Client) ListACLs() ([]*sarama.ResourceAcls, error) {
	broker, err := c.availableBroker()
	if err != nil {
		return nil, err
	}
	err = c.client.RefreshMetadata()
	if err != nil {
		return nil, err
	}
	allResources := []*sarama.DescribeAclsRequest{
		&sarama.DescribeAclsRequest{
			AclFilter: sarama.AclFilter{
				ResourceType:   sarama.AclResourceTopic,
				PermissionType: sarama.AclPermissionAny,
				Operation:      sarama.AclOperationAny,
			},
		},
		&sarama.DescribeAclsRequest{
			AclFilter: sarama.AclFilter{
				ResourceType:   sarama.AclResourceGroup,
				PermissionType: sarama.AclPermissionAny,
				Operation:      sarama.AclOperationAny,
			},
		},
		&sarama.DescribeAclsRequest{
			AclFilter: sarama.AclFilter{
				ResourceType:   sarama.AclResourceCluster,
				PermissionType: sarama.AclPermissionAny,
				Operation:      sarama.AclOperationAny,
			},
		},
		&sarama.DescribeAclsRequest{
			AclFilter: sarama.AclFilter{
				ResourceType:   sarama.AclResourceTransactionalID,
				PermissionType: sarama.AclPermissionAny,
				Operation:      sarama.AclOperationAny,
			},
		},
	}
	res := []*sarama.ResourceAcls{}

	for _, r := range allResources {
		aclsR, err := broker.DescribeAcls(r)
		if err != nil {
			return nil, err
		}

		if err == nil {
			if aclsR.Err != sarama.ErrNoError {
				return nil, fmt.Errorf("%s", aclsR.Err)
			}
		}

		for _, a := range aclsR.ResourceAcls {
			res = append(res, a)
		}
	}
	return res, err
}

func stringToACLPermissionType(in string) sarama.AclPermissionType {
	switch in {
	case "Unknown":
		return sarama.AclPermissionUnknown
	case "Any":
		return sarama.AclPermissionAny
	case "Deny":
		return sarama.AclPermissionDeny
	case "Allow":
		return sarama.AclPermissionAllow
	}
	return unknownConversion
}

func stringToOperation(in string) sarama.AclOperation {
	switch in {
	case "Unknown":
		return sarama.AclOperationUnknown
	case "Any":
		return sarama.AclOperationAny
	case "All":
		return sarama.AclOperationAll
	case "Read":
		return sarama.AclOperationRead
	case "Write":
		return sarama.AclOperationWrite
	case "Create":
		return sarama.AclOperationCreate
	case "Delete":
		return sarama.AclOperationDelete
	case "Alter":
		return sarama.AclOperationAlter
	case "Describe":
		return sarama.AclOperationDescribe
	case "ClusterAction":
		return sarama.AclOperationClusterAction
	case "DescribeConfigs":
		return sarama.AclOperationDescribeConfigs
	case "AlterConfigs":
		return sarama.AclOperationAlterConfigs
	case "IdempotentWrite":
		return sarama.AclOperationIdempotentWrite
	}
	return unknownConversion
}

func stringToACLResouce(in string) sarama.AclResourceType {
	switch in {
	case "Unknown":
		return sarama.AclResourceUnknown
	case "Any":
		return sarama.AclResourceAny
	case "Topic":
		return sarama.AclResourceTopic
	case "Group":
		return sarama.AclResourceGroup
	case "Cluster":
		return sarama.AclResourceCluster
	case "TransactionalID":
		return sarama.AclResourceTransactionalID
	}
	return unknownConversion
}
