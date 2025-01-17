/*
Set of wrappers around Ceph APIs.
*/
package rados

import (
	_ "github.com/ceph/go-ceph/rados"
	_ "github.com/ceph/go-ceph/rados/radosstriper"
	_ "github.com/ceph/go-ceph/cephfs"
	_ "github.com/ceph/go-ceph/rbd"
)
