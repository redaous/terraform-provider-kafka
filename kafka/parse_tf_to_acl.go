package kafka

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
)

type RawACL struct {
	ACL      ACL      `json:"acl"`
	Resource Resource `json:"resource"`
}

func (a RawACL) String() string {
	return strings.Join([]string{a.ACL.Principal, a.ACL.Host, a.ACL.Operation, a.ACL.PermissionType, a.Resource.Type, a.Resource.Name}, "|")
}

func (s RawACL) ToACLCreation() (*sarama.AclCreation, error) {
	acl := &sarama.AclCreation{}

	op := stringToOperation(s.ACL.Operation)
	if op == unknownConversion {
		return acl, fmt.Errorf("Unknown operation: %s", s.ACL.Operation)
	}
	pType := stringToACLPermissionType(s.ACL.PermissionType)
	if pType == unknownConversion {
		return acl, fmt.Errorf("Unknown permission type: %s", s.ACL.PermissionType)
	}
	rType := stringToACLResouce(s.Resource.Type)
	if rType == unknownConversion {
		return acl, fmt.Errorf("Unknown resource type: %s", s.Resource.Type)
	}

	acl.Acl = sarama.Acl{
		Principal:      s.ACL.Principal,
		Host:           s.ACL.Host,
		Operation:      op,
		PermissionType: pType,
	}

	acl.Resource = sarama.Resource{
		ResourceType: rType,
		ResourceName: s.Resource.Name,
	}

	return acl, nil
}

func tfToACLFilter(s RawACL) (sarama.AclFilter, error) {
	f := sarama.AclFilter{
		Principal:    &s.ACL.Principal,
		Host:         &s.ACL.Host,
		ResourceName: &s.Resource.Name,
	}

	op := stringToOperation(s.ACL.Operation)
	if op == unknownConversion {
		return f, fmt.Errorf("Unknown operation: %s", s.ACL.Operation)
	}
	f.Operation = op

	pType := stringToACLPermissionType(s.ACL.PermissionType)
	if pType == unknownConversion {
		return f, fmt.Errorf("Unknown permission type: %s", s.ACL.PermissionType)
	}
	f.PermissionType = pType

	rType := stringToACLResouce(s.Resource.Type)
	if rType == unknownConversion {
		return f, fmt.Errorf("Unknown resource type: %s", s.Resource.Type)
	}
	f.ResourceType = rType

	return f, nil
}
