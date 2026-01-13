package com.databricks.dicer.friend

import com.databricks.dicer.external.{ResourceAddress, Slice}

/**
 * Along with a `slice`, optionally provides information about the resource containing state for it.
 * If state transfer is enabled for the target AND the state provider information is available
 * (the Assigner may choose to drop this information), then `stateProviderResourceOpt` will be
 * defined and can be used to determine where to fetch state from when this Slice is newly assigned.
 * Note that the [[ResourceAddress]] corresponds to the service's port (i.e. the `selfPort` passed
 * to [[Slicelet.start]]), and the caller is responsible for determining how to actually transfer
 * state from that resource.
 */
final case class SliceWithStateProvider private[dicer] (
    slice: Slice,
    stateProviderResourceOpt: Option[ResourceAddress])
