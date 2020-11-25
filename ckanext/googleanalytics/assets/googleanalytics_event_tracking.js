/* global gtag */
// Add Google Analytics Event Tracking to resource download links.
this.ckan.module("google-analytics", function(jQuery, _) {
  "use strict";
  return {
    options: {
      googleanalytics_resource_prefix: ""
    },
    initialize: function() {
      jQuery("a.resource-url-analytics").on("click", function() {
        var resource_url = encodeURIComponent(jQuery(this).prop("href"));
        var resource_id = jQuery(this).attr('resource_id');
        if (resource_url) {
          gtag("event", 'file_download', {
            event_category: "Resource",
            event_label: "Download",
            value: resource_id+'|'+resource_url,
          });
        }
      });

      jQuery(".dataset-heading a").on("click", function() {
        var dataset_url = encodeURIComponent(jQuery(this).prop('href'));
        if (dataset_url) {
          gtag('event', 'click', {
            event_category: 'Dataset',
            event_label: 'CKAN_Dataset_view',
            value: dataset_url,
          });
        }
      });

      jQuery(".dataset-resource-text a:eq(1)").on("click", function() {
        var resource_url = encodeURIComponent(jQuery(this).prop('href'));
        if (resource_url) {
          gtag('event', 'view', {
            event_category: 'Resource',
            event_label: 'CKAN_Resource_view',
            value: resource_url,
          });
        }
      });

    }
  };
});
